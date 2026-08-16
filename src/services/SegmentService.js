/**
 * SegmentService
 *
 * Manages customer segment state: WARM → HOT → HOTTEST → CONVERTED.
 *
 * Rules (all config-driven, nothing hardcoded):
 *  - Promotion: one-directional up, based on score thresholds (segment_rules table)
 *  - Instant-flag: certain events force HOTTEST regardless of score (instant_flag_events table)
 *  - Demotion: ONLY on explicit `journey_stop` event — never from score decay alone
 *  - Pause: `test_ride_booking` sets is_paused = 1 on customer_scores
 *
 * Segment order (used for one-directional enforcement):
 *   WARM(1) < HOT(2) < HOTTEST(3) < CONVERTED(4)
 */

import { query, getConnection } from '../lib/db.js';
import { getCachedScoreConfig, setCachedScoreConfig } from '../lib/redis.js';
import { markLeadScored } from './LifecycleService.js';
import { dispatchSignal } from '../lib/engageSignal.js';
import logger from '../lib/logger.js';

const COMPONENT = 'SegmentService';

// Segment rank — higher = further along the funnel
const SEGMENT_RANK = {
  WARM:      1,
  HOT:       2,
  HOTTEST:   3,
  CONVERTED: 4,
};

// Events that trigger a messaging pause
const PAUSE_EVENTS = new Set(['test_ride_booking', 'book_bike']);

// The explicit demotion event
const DEMOTION_EVENT = 'journey_stop';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Recompute and update segment after a score change or event.
 *
 * Call this after every scoreEvent() and after decay.
 *
 * @param {object} params
 * @param {number} params.customerId
 * @param {string} params.source
 * @param {string} params.eventName   The event that triggered this recompute
 * @param {number} params.currentScore  The current score AFTER applying points/decay
 */
export async function recompute({ customerId, source, eventName, currentScore }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    // Lock the current customer_scores row
    const [rows] = await conn.execute(
      `SELECT segment, is_paused FROM customer_scores
       WHERE customer_id = ? AND source = ?
       FOR UPDATE`,
      [customerId, source],
    );

    const currentSegment = rows[0]?.segment ?? null;

    // --- 1. Demotion via journey_stop ---
    if (eventName === DEMOTION_EVENT) {
      const demotedTo = demoteSegment(currentSegment);
      if (demotedTo !== currentSegment) {
        await applySegmentChange({ conn, customerId, source, from: currentSegment, to: demotedTo, changedBy: 'journey_stop', eventName });
      }
      await conn.commit();
      return;
    }

    // --- 2. Pause on conversion events ---
    if (PAUSE_EVENTS.has(eventName)) {
      await conn.execute(
        `UPDATE customer_scores SET is_paused = 1, paused_at = NOW()
         WHERE customer_id = ? AND source = ?`,
        [customerId, source],
      );
      logger.info('Customer paused on conversion', { component: COMPONENT, customerId, source, eventName });
    }

    // --- 3. Instant-flag override ---
    const instantTarget = await getInstantFlagTarget(source, eventName);
    if (instantTarget) {
      const newSegment = resolveUpward(currentSegment, instantTarget);
      if (newSegment !== currentSegment) {
        await applySegmentChange({ conn, customerId, source, from: currentSegment, to: newSegment, changedBy: 'instant_flag', eventName });

        // First time a segment is assigned → mark LEAD_SCORED in lifecycle
        if (!currentSegment) {
          markLeadScored({ customerId, source }).catch((err) => {
            logger.error('markLeadScored failed', { component: COMPONENT, customerId, error: err.message });
          });
        }
      }
      await conn.commit();
      return;
    }

    // --- 4. Score-based promotion (only upward) ---
    const targetSegment = await getSegmentForScore(source, currentScore);
    if (targetSegment) {
      const newSegment = resolveUpward(currentSegment, targetSegment);
      if (newSegment !== currentSegment) {
        await applySegmentChange({ conn, customerId, source, from: currentSegment, to: newSegment, changedBy: 'event', eventName });

        // First time a segment is assigned → mark LEAD_SCORED in lifecycle
        if (!currentSegment) {
          markLeadScored({ customerId, source }).catch((err) => {
            logger.error('markLeadScored failed', { component: COMPONENT, customerId, error: err.message });
          });
        }
      }
    }

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    logger.error('Segment recompute failed', { component: COMPONENT, customerId, source, error: err.message });
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * Recompute segment after score decay.
 * Score decay NEVER demotes segment — it only re-evaluates for promotion.
 * Existing segment is preserved if score drops below current segment's threshold.
 *
 * @param {object} params
 * @param {number} params.customerId
 * @param {string} params.source
 * @param {number} params.currentScore
 */
export async function recomputeAfterDecay({ customerId, source, currentScore }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    const [rows] = await conn.execute(
      `SELECT segment FROM customer_scores WHERE customer_id = ? AND source = ? FOR UPDATE`,
      [customerId, source],
    );

    const currentSegment = rows[0]?.segment ?? null;

    // Score-based target — but only apply if it's an upgrade
    const targetSegment = await getSegmentForScore(source, currentScore);
    if (targetSegment) {
      const newSegment = resolveUpward(currentSegment, targetSegment);
      if (newSegment !== currentSegment) {
        await applySegmentChange({ conn, customerId, source, from: currentSegment, to: newSegment, changedBy: 'decay', eventName: null });
      }
      // If target is lower than current, we intentionally do nothing (decay doesn't demote)
    }

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    logger.error('Segment recompute after decay failed', { component: COMPONENT, customerId, source, error: err.message });
    throw err;
  } finally {
    conn.release();
  }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Write segment change to customer_scores and log to segment_history.
 */
async function applySegmentChange({ conn, customerId, source, from, to, changedBy, eventName }) {
  await conn.execute(
    `UPDATE customer_scores SET segment = ?, updated_at = NOW()
     WHERE customer_id = ? AND source = ?`,
    [to, customerId, source],
  );

  await conn.execute(
    `INSERT INTO segment_history (customer_id, source, from_segment, to_segment, changed_by, event_name)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [customerId, source, from ?? null, to, changedBy, eventName ?? null],
  );

  // Dispatch segment signal to revolt-engage journey engine
  dispatchSignal({ customerId, signalType: `segment_${to.toLowerCase()}` });

  logger.info('Segment changed', { component: COMPONENT, customerId, source, from, to, changedBy, eventName });
}

/**
 * Resolve the new segment — only allows upward movement.
 * Returns the higher of currentSegment and targetSegment.
 */
function resolveUpward(currentSegment, targetSegment) {
  const currentRank = SEGMENT_RANK[currentSegment] ?? 0;
  const targetRank  = SEGMENT_RANK[targetSegment]  ?? 0;
  return targetRank > currentRank ? targetSegment : currentSegment;
}

/**
 * Demote one level — only used for journey_stop.
 * HOTTEST → HOT, HOT → WARM, WARM stays WARM.
 */
function demoteSegment(currentSegment) {
  const ladder = ['WARM', 'HOT', 'HOTTEST', 'CONVERTED'];
  const idx = ladder.indexOf(currentSegment);
  if (idx <= 0) return currentSegment;
  return ladder[idx - 1];
}

/**
 * Look up which segment a given score maps to.
 * Uses Redis-cached segment_rules.
 */
async function getSegmentForScore(source, score) {
  if (score <= 0) return null;

  const rules = await getSegmentRules(source);

  for (const rule of rules) {
    const withinMax = rule.max_score === null || score <= rule.max_score;
    if (score >= rule.min_score && withinMax) {
      return rule.segment_name;
    }
  }
  return null;
}

/**
 * Check if an event is an instant-flag event and return its target segment.
 * Returns null if not an instant-flag event.
 */
async function getInstantFlagTarget(source, eventName) {
  const flags = await getInstantFlags(source);
  return flags[eventName] ?? null;
}

// ---------------------------------------------------------------------------
// Cached config loaders
// ---------------------------------------------------------------------------

const SEGMENT_RULES_CACHE_KEY = (source) => `segment_rules:${source}`;
const INSTANT_FLAGS_CACHE_KEY  = (source) => `instant_flags:${source}`;
// Reuse the same Redis TTL as score config (5 min)

async function getSegmentRules(source) {
  const cacheKey = SEGMENT_RULES_CACHE_KEY(source);
  const cached = await getCachedScoreConfig(cacheKey);
  if (cached) return cached;

  const rows = await query(
    `SELECT segment_name, min_score, max_score, daily_touch_cap
     FROM segment_rules
     WHERE source = ? AND is_active = 1
     ORDER BY min_score ASC`,
    [source],
  );

  await setCachedScoreConfig(cacheKey, rows).catch(() => {});
  return rows;
}

async function getInstantFlags(source) {
  const cacheKey = INSTANT_FLAGS_CACHE_KEY(source);
  const cached = await getCachedScoreConfig(cacheKey);
  if (cached) return cached;

  const rows = await query(
    `SELECT event_name, target_segment FROM instant_flag_events
     WHERE source = ? AND is_active = 1`,
    [source],
  );

  const map = Object.fromEntries(rows.map((r) => [r.event_name, r.target_segment]));
  await setCachedScoreConfig(cacheKey, map).catch(() => {});
  return map;
}

export default { recompute, recomputeAfterDecay };
