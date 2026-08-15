/**
 * LifecycleService
 *
 * Tracks which stage of the purchase funnel each customer is in.
 *
 * Stages (ordered):
 *   LEAD_FILLED → LEAD_SCORED → TEST_RIDE_BOOKED → TEST_RIDE_SCHEDULED
 *   → TEST_RIDE_COMPLETED | NO_SHOW → BOOKING_STARTED → BOOKING_CREATED
 *   → RETAIL_COMPLETED
 *
 * Two tables:
 *   customer_lifecycle  — current stage per (customer, source), updated in-place
 *   lifecycle_history   — immutable log of every transition (append-only)
 *
 * Transition rules live in lifecycle_stage_triggers (DB, Redis-cached).
 * Nothing is hardcoded here.
 *
 * Regression: NO_SHOW is the only stage where a customer can move backwards
 * (re-book a test ride after a no-show). All other transitions only go forward
 * unless is_regression_allowed = 1 on the trigger.
 */

import { query, getConnection } from '../lib/db.js';
import { getCachedScoreConfig, setCachedScoreConfig } from '../lib/redis.js';
import logger from '../lib/logger.js';

const COMPONENT = 'LifecycleService';

// Ordered stage list — used to enforce forward-only movement
const STAGE_ORDER = [
  'LEAD_FILLED',
  'LEAD_SCORED',
  'TEST_RIDE_BOOKED',
  'TEST_RIDE_SCHEDULED',
  'TEST_RIDE_COMPLETED',
  'NO_SHOW',
  'BOOKING_STARTED',
  'BOOKING_CREATED',
  'RETAIL_COMPLETED',
];

const stageRank = (stage) => STAGE_ORDER.indexOf(stage); // -1 if unknown

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Advance lifecycle stage based on an incoming event.
 * Called from eventsConsumer after scoring.
 *
 * @param {object} params
 * @param {number} params.customerId
 * @param {string} params.source
 * @param {string} params.eventName
 * @param {Date}   params.eventTime
 */
export async function advance({ customerId, source, eventName, eventTime }) {
  const trigger = await getTriggerForEvent(source, eventName);

  if (!trigger) {
    // This event doesn't drive a lifecycle transition — nothing to do
    return;
  }

  const toStage = trigger.to_stage;
  const regressionAllowed = !!trigger.is_regression_allowed;

  await transition({ customerId, source, toStage, eventName, eventTime, regressionAllowed });
}

/**
 * Directly set LEAD_SCORED when a customer gets their first segment.
 * Called from SegmentService when first segment is assigned.
 *
 * @param {object} params
 * @param {number} params.customerId
 * @param {string} params.source
 * @param {Date}   [params.eventTime]
 */
export async function markLeadScored({ customerId, source, eventTime }) {
  await transition({
    customerId,
    source,
    toStage: 'LEAD_SCORED',
    eventName: '__segment_assigned__',
    eventTime: eventTime ?? new Date(),
    regressionAllowed: false,
  });
}

// ---------------------------------------------------------------------------
// Core transition logic
// ---------------------------------------------------------------------------

/**
 * Execute a stage transition:
 *  1. Load current stage
 *  2. Check if transition is allowed (forward-only unless regression permitted)
 *  3. Close out current stage in lifecycle_history (set exited_at)
 *  4. Update customer_lifecycle to new stage
 *  5. Insert new lifecycle_history row for new stage
 */
async function transition({ customerId, source, toStage, eventName, eventTime, regressionAllowed }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    // Lock current lifecycle row
    const [rows] = await conn.execute(
      `SELECT current_stage, entered_at FROM customer_lifecycle
       WHERE customer_id = ? AND source = ?
       FOR UPDATE`,
      [customerId, source],
    );

    const current      = rows[0] ?? null;
    const currentStage = current?.current_stage ?? null;

    // Already in this stage — idempotent, skip
    if (currentStage === toStage) {
      await conn.rollback();
      return;
    }

    // Enforce forward-only unless regression is explicitly allowed
    if (currentStage && !regressionAllowed) {
      const currentRank = stageRank(currentStage);
      const toRank      = stageRank(toStage);

      // Unknown stages (rank = -1) are always allowed through
      if (currentRank !== -1 && toRank !== -1 && toRank < currentRank) {
        logger.debug('Lifecycle regression blocked', {
          component: COMPONENT, customerId, source, currentStage, toStage, eventName,
        });
        await conn.rollback();
        return;
      }
    }

    // Close the current open history row
    if (currentStage) {
      await conn.execute(
        `UPDATE lifecycle_history
         SET exited_at = ?
         WHERE customer_id = ? AND source = ? AND stage = ? AND exited_at IS NULL`,
        [eventTime, customerId, source, currentStage],
      );
    }

    // Upsert customer_lifecycle
    await conn.execute(
      `INSERT INTO customer_lifecycle (customer_id, source, current_stage, previous_stage, entered_at)
       VALUES (?, ?, ?, NULL, ?)
       ON DUPLICATE KEY UPDATE
         previous_stage = current_stage,
         current_stage  = VALUES(current_stage),
         entered_at     = VALUES(entered_at),
         updated_at     = NOW()`,
      [customerId, source, toStage, eventTime],
    );

    // Append to lifecycle_history
    await conn.execute(
      `INSERT INTO lifecycle_history (customer_id, source, stage, event_name, entered_at)
       VALUES (?, ?, ?, ?, ?)`,
      [customerId, source, toStage, eventName, eventTime],
    );

    await conn.commit();

    logger.info('Lifecycle transition', {
      component: COMPONENT, customerId, source,
      from: currentStage, to: toStage, eventName,
    });
  } catch (err) {
    await conn.rollback();
    logger.error('Lifecycle transition failed', {
      component: COMPONENT, customerId, source, toStage, eventName, error: err.message,
    });
    throw err;
  } finally {
    conn.release();
  }
}

// ---------------------------------------------------------------------------
// Config loader (Redis-cached)
// ---------------------------------------------------------------------------

/**
 * Returns the trigger config for (source, event_name), or null if none.
 */
async function getTriggerForEvent(source, eventName) {
  const triggers = await getTriggersForSource(source);
  return triggers[eventName] ?? null;
}

async function getTriggersForSource(source) {
  const cacheKey = `lifecycle_triggers:${source}`;
  const cached   = await getCachedScoreConfig(cacheKey);
  if (cached) return cached;

  const rows = await query(
    `SELECT event_name, to_stage, is_regression_allowed
     FROM lifecycle_stage_triggers
     WHERE source = ? AND is_active = 1`,
    [source],
  );

  const map = Object.fromEntries(rows.map((r) => [r.event_name, r]));
  setCachedScoreConfig(cacheKey, map).catch(() => {});
  return map;
}

export default { advance, markLeadScored };
