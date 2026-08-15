/**
 * IdentityService
 *
 * Handles everything related to knowing who an anonymous visitor is.
 *
 * Responsibilities:
 *  1. Look up anonymous_profiles by anonymous_id (Redis → MySQL)
 *  2. Create customers on form-submission events (test ride, book bike, login)
 *  3. Link anonymous_id → customer_id (and log to identity_merges)
 *  4. Handle cross-source merges: same person arrives via two different anonymous_ids
 *  5. Trigger one-time backfill scoring the moment a visitor is first identified
 *
 * Redis strategy:
 *  - Every resolved lookup is cached: anon:{source}:{anonymous_id} → customerId
 *  - "null" string is cached for anonymous visitors so we don't hit MySQL each time
 *  - Cache is invalidated immediately when identity is resolved
 */

import { query, getConnection } from '../lib/db.js';
import {
  getCachedAnonymousProfile,
  setCachedAnonymousProfile,
  invalidateCachedAnonymousProfile,
} from '../lib/redis.js';
import { backfillScore } from './ScoringService.js';
import logger from '../lib/logger.js';

const COMPONENT = 'IdentityService';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Resolve the customer_id for an anonymous_id.
 *
 * Flow:
 *   Redis hit → return immediately
 *   Redis miss → MySQL lookup → cache result → return
 *
 * Returns:
 *   number  → customer_id (visitor is known)
 *   null    → visitor is anonymous (no customer linked yet)
 *
 * @param {string} source
 * @param {string} anonymousId
 * @returns {Promise<number|null>}
 */
export async function resolveCustomerId(source, anonymousId) {
  // 1. Redis
  const cached = await getCachedAnonymousProfile(source, anonymousId);
  if (cached !== undefined) return cached; // undefined = miss; null = anonymous; number = known

  // 2. MySQL
  const rows = await query(
    `SELECT customer_id FROM anonymous_profiles
     WHERE anonymous_id = ? AND source = ?
     LIMIT 1`,
    [anonymousId, source],
  );

  if (!rows.length) {
    // Completely new visitor — insert an anonymous profile row and cache as null
    await upsertAnonymousProfile(anonymousId, source, null);
    await setCachedAnonymousProfile(source, anonymousId, null);
    return null;
  }

  const customerId = rows[0].customer_id ?? null;
  await setCachedAnonymousProfile(source, anonymousId, customerId);
  return customerId;
}

/**
 * Identify a visitor from a form-submission event.
 *
 * Logic:
 *  1. Try to find existing customer by phone, then by email
 *  2. If found AND it's a different anonymous_id → log as cross-source merge
 *  3. If not found → create new customer
 *  4. Link the anonymous_id to the customer
 *  5. If this is the FIRST time this anonymous_id gets identified → run backfill scoring
 *
 * @param {object} params
 * @param {string} params.source
 * @param {string} params.anonymousId
 * @param {string} [params.phone]
 * @param {string} [params.email]
 * @param {string} [params.name]
 * @returns {Promise<{ customerId: number, isNew: boolean }>}
 */
export async function identifyVisitor({ source, anonymousId, phone, email, name }) {
  if (!phone && !email) {
    logger.warn('identifyVisitor called with no phone or email — skipping', {
      component: COMPONENT, source, anonymousId,
    });
    return null;
  }

  // 1. Find existing customer
  const existing = await findCustomer({ phone, email });

  let customerId;
  let isNew = false;
  let mergeReason = null;

  if (existing) {
    customerId = existing.id;

    // Detect cross-source merge: the same person came from somewhere else
    if (phone && existing.phone && existing.phone === phone) mergeReason = 'phone_match';
    else if (email && existing.email && existing.email === email) mergeReason = 'email_match';

    // Update any missing fields on the customer (e.g. now we also have their name)
    await updateCustomerIfNeeded(customerId, { phone, email, name });
  } else {
    // 2. Create new customer
    customerId = await createCustomer({ phone, email, name });
    isNew = true;
  }

  // 3. Link anonymous_id → customer, get back whether this was first identification
  const wasAlreadyIdentified = await linkAnonymousId({
    anonymousId,
    source,
    customerId,
    mergeReason: mergeReason ?? (isNew ? 'new_customer' : 'existing_customer_linked'),
  });

  // 4. First-time identification → run backfill scoring
  if (!wasAlreadyIdentified) {
    // Fire-and-forget backfill — don't block event processing
    backfillScore({ customerId, anonymousId, source }).catch((err) => {
      logger.error('Backfill scoring failed', {
        component: COMPONENT, customerId, anonymousId, source, error: err.message,
      });
    });
  }

  logger.info('Visitor identified', {
    component: COMPONENT, customerId, anonymousId, source, isNew,
    wasAlreadyIdentified, mergeReason,
  });

  return { customerId, isNew };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Find a customer by phone (priority) or email.
 */
async function findCustomer({ phone, email }) {
  if (phone) {
    const rows = await query(
      `SELECT id, phone, email FROM customers WHERE phone = ? LIMIT 1`,
      [phone],
    );
    if (rows.length) return rows[0];
  }

  if (email) {
    const rows = await query(
      `SELECT id, phone, email FROM customers WHERE email = ? LIMIT 1`,
      [email],
    );
    if (rows.length) return rows[0];
  }

  return null;
}

/**
 * Create a new customer row and return the new id.
 */
async function createCustomer({ phone, email, name }) {
  const result = await query(
    `INSERT INTO customers (phone, email, name) VALUES (?, ?, ?)`,
    [phone ?? null, email ?? null, name ?? null],
  );
  logger.info('New customer created', { component: COMPONENT, customerId: result.insertId, phone, email });
  return result.insertId;
}

/**
 * Fill in null fields on an existing customer (never overwrite existing data).
 */
async function updateCustomerIfNeeded(customerId, { phone, email, name }) {
  await query(
    `UPDATE customers
     SET phone = COALESCE(phone, ?),
         email = COALESCE(email, ?),
         name  = COALESCE(name,  ?)
     WHERE id = ?`,
    [phone ?? null, email ?? null, name ?? null, customerId],
  );
}

/**
 * Insert or update anonymous_profiles to link anonymous_id → customerId.
 *
 * Returns true if the anonymous_id was ALREADY identified before this call
 * (identified_at was already set), false if this is the first identification.
 *
 * identified_at acts as the backfill gate — set only on first link.
 */
async function linkAnonymousId({ anonymousId, source, customerId, mergeReason }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    // Check current state
    const [rows] = await conn.execute(
      `SELECT id, customer_id, identified_at FROM anonymous_profiles
       WHERE anonymous_id = ? AND source = ?
       FOR UPDATE`,
      [anonymousId, source],
    );

    const existing = rows[0];
    const wasAlreadyIdentified = !!(existing?.identified_at);

    if (existing) {
      // Update link (handles re-link / cross-source merge)
      await conn.execute(
        `UPDATE anonymous_profiles
         SET customer_id   = ?,
             identified_at = COALESCE(identified_at, NOW()),
             updated_at    = NOW()
         WHERE anonymous_id = ? AND source = ?`,
        [customerId, anonymousId, source],
      );
    } else {
      // First time we've seen this anonymous_id
      await conn.execute(
        `INSERT INTO anonymous_profiles (anonymous_id, source, customer_id, identified_at)
         VALUES (?, ?, ?, NOW())`,
        [anonymousId, source, customerId],
      );
    }

    // Always log the identity merge event
    await conn.execute(
      `INSERT INTO identity_merges (primary_customer_id, anonymous_id, source, reason)
       VALUES (?, ?, ?, ?)`,
      [customerId, anonymousId, source, mergeReason],
    );

    await conn.commit();

    // Bust the Redis cache so next event reads the fresh customer_id
    await invalidateCachedAnonymousProfile(source, anonymousId);

    return wasAlreadyIdentified;
  } catch (err) {
    await conn.rollback();
    logger.error('Failed to link anonymous_id', {
      component: COMPONENT, anonymousId, source, customerId, error: err.message,
    });
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * Ensure an anonymous_profiles row exists for a brand-new visitor.
 */
async function upsertAnonymousProfile(anonymousId, source, customerId) {
  await query(
    `INSERT INTO anonymous_profiles (anonymous_id, source, customer_id)
     VALUES (?, ?, ?)
     ON DUPLICATE KEY UPDATE updated_at = NOW()`,
    [anonymousId, source, customerId ?? null],
  );
}

export default { resolveCustomerId, identifyVisitor };
