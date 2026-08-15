/**
 * Decay Worker
 *
 * Scheduled job that handles score decay for truly dormant leads —
 * customers who never come back and therefore never trigger the
 * real-time decay path in ScoringService.
 *
 * Runs every DECAY_INTERVAL_MS (default: every 12 hours).
 *
 * Per-customer logic (mirrors ScoringService.applyDecayIfNeeded):
 *  0–20 days inactive  → skip
 *  21–45 days inactive → reduce score by 30%
 *  46+ days inactive   → reset to 0
 *
 * Segment is NEVER demoted by decay (enforced in SegmentService).
 *
 * Safety:
 *  - Processes customers in batches (BATCH_SIZE) to avoid locking the DB
 *  - Each customer is only decayed once per decay window (checked via
 *    last __decay__ score_events row)
 */

import { query, getConnection } from '../lib/db.js';
import { getCachedScoreConfig, setCachedScoreConfig } from '../lib/redis.js';
import { recomputeAfterDecay } from '../services/SegmentService.js';
import logger from '../lib/logger.js';

const WORKER_NAME        = 'decay-worker';
const DECAY_INTERVAL_MS  = 12 * 60 * 60 * 1000; // 12 hours
const BATCH_SIZE         = 200;

let intervalHandle  = null;
let isShuttingDown  = false;
let isRunning       = false; // prevent overlapping runs

// ---------------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------------

export async function start() {
  logger.info('Starting decay worker', { component: WORKER_NAME, intervalMs: DECAY_INTERVAL_MS });

  // Run once immediately on start, then on schedule
  await runDecayCycle();

  intervalHandle = setInterval(async () => {
    if (isShuttingDown) return;
    await runDecayCycle();
  }, DECAY_INTERVAL_MS);
}

export async function stop() {
  isShuttingDown = true;
  if (intervalHandle) {
    clearInterval(intervalHandle);
    intervalHandle = null;
  }
  logger.info('Decay worker stopped', { component: WORKER_NAME });
}

// ---------------------------------------------------------------------------
// Decay cycle
// ---------------------------------------------------------------------------

async function runDecayCycle() {
  if (isRunning) {
    logger.warn('Decay cycle already running — skipping this tick', { component: WORKER_NAME });
    return;
  }

  isRunning = true;
  const started = Date.now();
  logger.info('Decay cycle started', { component: WORKER_NAME });

  try {
    // Load all active sources that have decay rules
    const sources = await getActiveSources();

    for (const source of sources) {
      if (isShuttingDown) break;
      await decaySource(source);
    }

    logger.info('Decay cycle complete', {
      component: WORKER_NAME,
      durationMs: Date.now() - started,
      sources,
    });
  } catch (err) {
    logger.error('Decay cycle failed', { component: WORKER_NAME, error: err.message, stack: err.stack });
  } finally {
    isRunning = false;
  }
}

/**
 * Process all customers for a given source that are past the decay threshold.
 */
async function decaySource(source) {
  const rules = await getDecayRulesForSource(source);

  // Only care about rules that actually do something (decay_value > 0 or reset)
  const activeRules = rules.filter((r) => r.decay_type === 'reset' || r.decay_value > 0);
  if (!activeRules.length) return;

  // Minimum days before any decay kicks in
  const minDays = Math.min(...activeRules.map((r) => r.days_from));

  let offset = 0;
  let processed = 0;

  while (!isShuttingDown) {
    // Find customers whose last_seen_at is past the minimum decay threshold
    // and who haven't been decayed in the last 12 hours (avoid double-decay)
    const customers = await query(
      `SELECT cs.customer_id, cs.score, cs.last_seen_at
       FROM customer_scores cs
       WHERE cs.source = ?
         AND cs.score > 0
         AND cs.last_seen_at IS NOT NULL
         AND cs.last_seen_at < DATE_SUB(NOW(), INTERVAL ? DAY)
         AND NOT EXISTS (
           SELECT 1 FROM score_events se
           WHERE se.customer_id = cs.customer_id
             AND se.source      = ?
             AND se.event_name  = '__decay__'
             AND se.created_at  > DATE_SUB(NOW(), INTERVAL 12 HOUR)
         )
       ORDER BY cs.customer_id
       LIMIT ? OFFSET ?`,
      [source, minDays, source, BATCH_SIZE, offset],
    );

    if (!customers.length) break;

    for (const customer of customers) {
      if (isShuttingDown) break;
      await decayCustomer({ customer, source, rules });
      processed++;
    }

    offset += BATCH_SIZE;

    // Small pause between batches to be kind to the DB
    await sleep(100);
  }

  if (processed > 0) {
    logger.info('Source decay complete', { component: WORKER_NAME, source, processed });
  }
}

/**
 * Apply the correct decay rule to a single customer.
 */
async function decayCustomer({ customer, source, rules }) {
  const { customer_id: customerId, score: currentScore, last_seen_at } = customer;

  const now          = new Date();
  const lastSeen     = new Date(last_seen_at);
  const daysInactive = Math.floor((now - lastSeen) / (1000 * 60 * 60 * 24));

  const rule = rules.find((r) => {
    const withinUpper = r.days_to === null || daysInactive <= r.days_to;
    return daysInactive >= r.days_from && withinUpper;
  });

  if (!rule || (rule.decay_type === 'percent' && rule.decay_value === 0)) return;

  let newScore;
  if (rule.decay_type === 'reset') {
    newScore = rule.min_score;
  } else {
    newScore = Math.max(rule.min_score, Math.floor(currentScore * (1 - rule.decay_value / 100)));
  }

  const decayDelta = newScore - currentScore;
  if (decayDelta === 0) return;

  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    await conn.execute(
      `UPDATE customer_scores SET score = ?, updated_at = NOW()
       WHERE customer_id = ? AND source = ?`,
      [newScore, customerId, source],
    );

    await conn.execute(
      `INSERT INTO score_events
         (customer_id, anonymous_id, source, event_name, event_id, score_delta, is_backfill, event_time)
       VALUES (?, '', ?, '__decay__', NULL, ?, 0, NOW())`,
      [customerId, source, decayDelta],
    );

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    logger.error('Failed to decay customer', { component: WORKER_NAME, customerId, source, error: err.message });
    return;
  } finally {
    conn.release();
  }

  // Recompute segment (decay never demotes — enforced inside SegmentService)
  await recomputeAfterDecay({ customerId, source, currentScore: newScore }).catch((err) => {
    logger.error('Segment recompute failed after decay', { component: WORKER_NAME, customerId, error: err.message });
  });

  logger.debug('Customer decayed', { component: WORKER_NAME, customerId, source, daysInactive, decayDelta, newScore });
}

// ---------------------------------------------------------------------------
// Config loaders (Redis-cached)
// ---------------------------------------------------------------------------

async function getActiveSources() {
  const rows = await query(
    `SELECT DISTINCT source FROM score_decay_rules WHERE is_active = 1`,
  );
  return rows.map((r) => r.source);
}

async function getDecayRulesForSource(source) {
  const cacheKey = `decay_rules:${source}`;
  const cached   = await getCachedScoreConfig(cacheKey);
  if (cached) return cached;

  const rows = await query(
    `SELECT days_from, days_to, decay_type, decay_value, min_score
     FROM score_decay_rules
     WHERE source = ? AND is_active = 1
     ORDER BY days_from ASC`,
    [source],
  );

  setCachedScoreConfig(cacheKey, rows).catch(() => {});
  return rows;
}

// ---------------------------------------------------------------------------
// Util
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export default { start, stop };
