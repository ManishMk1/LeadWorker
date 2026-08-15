/**
 * ScoringService
 *
 * Source-agnostic scoring logic. No points hardcoded — all values come from
 * the `source_event_scores` MySQL table, cached in Redis per source.
 *
 * Decay rules (from score_decay_rules table):
 *   0–20 days inactive  → no decay
 *   21–45 days inactive → drop 30%
 *   46+ days inactive   → reset to 0
 *
 * Decay is applied BEFORE adding new points on every event ("recompute on
 * every new session"), matching the governance spec.
 *
 * After every score change, SegmentService.recompute() is called to keep
 * the customer's bucket (WARM/HOT/HOTTEST) up to date.
 */

import { query, getConnection } from '../lib/db.js';
import { queryClickHouse } from '../lib/clickhouse.js';
import { getCachedScoreConfig, setCachedScoreConfig } from '../lib/redis.js';
import { recompute as recomputeSegment, recomputeAfterDecay } from './SegmentService.js';
import logger from '../lib/logger.js';

const COMPONENT = 'ScoringService';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Score a single live event for a known customer.
 *
 * Order of operations:
 *  1. Load decay rules + check last_seen_at → apply decay if needed
 *  2. Look up event points
 *  3. Add points
 *  4. Update last_seen_at
 *  5. Recompute segment
 */
export async function scoreEvent({ customerId, anonymousId, source, eventName, eventId, eventTime }) {
  // 1. Apply decay based on inactivity since last event
  const { scoreAfterDecay: _scoreAfterDecay, decayApplied } = await applyDecayIfNeeded({ customerId, source, eventTime });

  // 2. Look up points for this event
  const scoreMap = await getScoreMapForSource(source);
  const points   = scoreMap[eventName] ?? null;

  if (points === null && !decayApplied) {
    logger.debug('No score config for event and no decay — skipping', { component: COMPONENT, source, eventName });
    return;
  }

  const scoreDelta = points ?? 0;

  // 3 & 4. Apply score delta + update last_seen_at in one transaction
  const finalScore = await applyScore({
    customerId, anonymousId, source, eventName, eventId,
    scoreDelta, eventTime, isBackfill: false,
  });

  if (points !== null) {
    logger.info('Event scored', { component: COMPONENT, customerId, source, eventName, points, finalScore });
  }

  // 5. Recompute segment (instant-flag, promotion, pause, demotion)
  await recomputeSegment({ customerId, source, eventName, currentScore: finalScore }).catch((err) => {
    logger.error('Segment recompute failed after scoreEvent', { component: COMPONENT, customerId, error: err.message });
  });
}

/**
 * One-time historical backfill when a visitor becomes known.
 * Queries ClickHouse for ALL past events of this anonymous_id, sums their
 * scores, and writes a single backfill entry.
 *
 * Guard: IdentityService sets `identified_at` before calling this, so it
 * only ever runs once per (anonymous_id, source).
 */
export async function backfillScore({ customerId, anonymousId, source }) {
  logger.info('Starting backfill scoring', { component: COMPONENT, customerId, anonymousId, source });

  const pastEvents = await fetchPastEvents(anonymousId, source);

  if (!pastEvents.length) {
    logger.info('No past events found for backfill', { component: COMPONENT, anonymousId, source });
    return;
  }

  const scoreMap = await getScoreMapForSource(source);

  let totalScore = 0;
  for (const evt of pastEvents) {
    totalScore += scoreMap[evt.event_name] ?? 0;
  }

  if (totalScore === 0) {
    logger.info('Backfill score is 0 — nothing to write', { component: COMPONENT, anonymousId, source });
    return;
  }

  const finalScore = await applyScore({
    customerId,
    anonymousId,
    source,
    eventName:  '__backfill__',
    eventId:    null,
    scoreDelta: totalScore,
    eventTime:  new Date(),
    isBackfill: true,
  });

  // Recompute segment after backfill
  await recomputeSegment({ customerId, source, eventName: '__backfill__', currentScore: finalScore }).catch(() => {});

  logger.info('Backfill complete', {
    component: COMPONENT, customerId, anonymousId, source,
    pastEventCount: pastEvents.length, totalScore,
  });
}

// ---------------------------------------------------------------------------
// Decay logic
// ---------------------------------------------------------------------------

/**
 * Check last_seen_at for the customer and apply decay if inactivity thresholds
 * are crossed. Writes a negative score_events row if decay is applied.
 *
 * Returns { scoreAfterDecay, decayApplied }.
 */
async function applyDecayIfNeeded({ customerId, source, eventTime }) {
  const rows = await query(
    `SELECT score, last_seen_at FROM customer_scores
     WHERE customer_id = ? AND source = ?`,
    [customerId, source],
  );

  if (!rows.length || rows[0].score <= 0 || !rows[0].last_seen_at) {
    return { scoreAfterDecay: rows[0]?.score ?? 0, decayApplied: false };
  }

  const { score: currentScore, last_seen_at } = rows[0];
  const lastSeen     = new Date(last_seen_at);
  const daysInactive = Math.floor((eventTime - lastSeen) / (1000 * 60 * 60 * 24));

  const decayRules = await getDecayRulesForSource(source);
  const rule = decayRules.find((r) => {
    const withinUpper = r.days_to === null || daysInactive <= r.days_to;
    return daysInactive >= r.days_from && withinUpper;
  });

  // No rule found or no-decay rule (decay_value = 0 and type = percent)
  if (!rule || (rule.decay_type === 'percent' && rule.decay_value === 0)) {
    return { scoreAfterDecay: currentScore, decayApplied: false };
  }

  let newScore;
  if (rule.decay_type === 'reset') {
    newScore = rule.min_score;
  } else {
    // percent
    newScore = Math.max(rule.min_score, Math.floor(currentScore * (1 - rule.decay_value / 100)));
  }

  const decayDelta = newScore - currentScore; // always negative or zero

  if (decayDelta === 0) {
    return { scoreAfterDecay: currentScore, decayApplied: false };
  }

  // Write decay as a score_events row + update customer_scores
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
       VALUES (?, '', ?, '__decay__', NULL, ?, 0, ?)`,
      [customerId, source, decayDelta, eventTime],
    );

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    logger.error('Failed to apply decay', { component: COMPONENT, customerId, source, error: err.message });
    throw err;
  } finally {
    conn.release();
  }

  // Recompute segment after decay (never demotes — enforced inside SegmentService)
  await recomputeAfterDecay({ customerId, source, currentScore: newScore }).catch(() => {});

  logger.info('Score decayed', {
    component: COMPONENT, customerId, source,
    daysInactive, rule: rule.decay_type, decayDelta, newScore,
  });

  return { scoreAfterDecay: newScore, decayApplied: true };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Upsert customer_scores (score + last_seen_at) and insert score_events audit row.
 * Returns the new running total score.
 */
async function applyScore({ customerId, anonymousId, source, eventName, eventId, scoreDelta, eventTime, isBackfill }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    await conn.execute(
      `INSERT INTO customer_scores (customer_id, source, score, last_seen_at)
       VALUES (?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         score        = score + VALUES(score),
         last_seen_at = VALUES(last_seen_at),
         updated_at   = NOW()`,
      [customerId, source, scoreDelta, eventTime],
    );

    await conn.execute(
      `INSERT INTO score_events
         (customer_id, anonymous_id, source, event_name, event_id, score_delta, is_backfill, event_time)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [customerId, anonymousId, source, eventName, eventId ?? null, scoreDelta, isBackfill ? 1 : 0, eventTime],
    );

    // Read back the final score
    const [scoreRows] = await conn.execute(
      `SELECT score FROM customer_scores WHERE customer_id = ? AND source = ?`,
      [customerId, source],
    );

    await conn.commit();
    return scoreRows[0]?.score ?? 0;
  } catch (err) {
    await conn.rollback();
    logger.error('Failed to apply score', { component: COMPONENT, customerId, eventName, error: err.message });
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * Returns { event_name: score } map for a source.
 * Redis → MySQL fallback.
 */
async function getScoreMapForSource(source) {
  const cached = await getCachedScoreConfig(source);
  if (cached) return cached;

  const rows = await query(
    `SELECT event_name, score FROM source_event_scores
     WHERE source = ? AND is_active = 1`,
    [source],
  );

  const scoreMap = Object.fromEntries(rows.map((r) => [r.event_name, r.score]));
  setCachedScoreConfig(source, scoreMap).catch(() => {});
  return scoreMap;
}

/**
 * Returns decay rules for a source, ordered by days_from ascending.
 * Redis → MySQL fallback.
 */
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

/**
 * Fetch all past events for an anonymous_id from ClickHouse.
 */
async function fetchPastEvents(anonymousId, source) {
  try {
    return await queryClickHouse(
      `SELECT event_name, event_id, created_at
       FROM rev_events
       WHERE anonymous_id = {anonymousId: String}
         AND source       = {source: String}
       ORDER BY created_at ASC`,
      { anonymousId, source },
    );
  } catch (err) {
    logger.error('ClickHouse fetch failed during backfill', {
      component: COMPONENT, anonymousId, source, error: err.message,
    });
    return [];
  }
}

export default { scoreEvent, backfillScore };
