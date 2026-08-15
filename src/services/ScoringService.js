/**
 * ScoringService
 *
 * Source-agnostic scoring logic. No points hardcoded — all values come from
 * the `source_event_scores` MySQL table, cached in Redis per source.
 *
 * Public API:
 *   scoreEvent({ customerId, anonymousId, source, eventName, eventId, eventTime })
 *   backfillScore({ customerId, anonymousId, source })
 */

import { query, getConnection } from '../lib/db.js';
import { queryClickHouse } from '../lib/clickhouse.js';
import { getCachedScoreConfig, setCachedScoreConfig } from '../lib/redis.js';
import logger from '../lib/logger.js';

const COMPONENT = 'ScoringService';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Score a single live event for a known customer.
 * Silently skips if the event has no configured score or is inactive.
 */
export async function scoreEvent({ customerId, anonymousId, source, eventName, eventId, eventTime }) {
  const scoreMap = await getScoreMapForSource(source);
  const points = scoreMap[eventName] ?? null;

  if (points === null) {
    logger.debug('No score config for event — skipping', { component: COMPONENT, source, eventName });
    return;
  }

  await applyScore({ customerId, anonymousId, source, eventName, eventId, scoreDelta: points, eventTime, isBackfill: false });

  logger.info('Event scored', { component: COMPONENT, customerId, source, eventName, points });
}

/**
 * One-time historical backfill when a visitor becomes known.
 * Queries ClickHouse for ALL past events of this anonymous_id, sums their
 * scores, and writes a single backfill entry — not one row per past event.
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

  await applyScore({
    customerId,
    anonymousId,
    source,
    eventName:  '__backfill__',
    eventId:    null,
    scoreDelta: totalScore,
    eventTime:  new Date(),
    isBackfill: true,
  });

  logger.info('Backfill complete', {
    component: COMPONENT, customerId, anonymousId, source,
    pastEventCount: pastEvents.length, totalScore,
  });
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Returns { event_name: score } map for a source.
 * Tries Redis first; falls back to MySQL and re-populates the cache.
 */
async function getScoreMapForSource(source) {
  // 1. Cache hit
  const cached = await getCachedScoreConfig(source);
  if (cached) return cached;

  // 2. Cache miss — load from MySQL
  const rows = await query(
    `SELECT event_name, score FROM source_event_scores
     WHERE source = ? AND is_active = 1`,
    [source],
  );

  const scoreMap = Object.fromEntries(rows.map((r) => [r.event_name, r.score]));

  // 3. Populate cache (fire-and-forget — don't block scoring on cache write)
  setCachedScoreConfig(source, scoreMap).catch(() => {});

  return scoreMap;
}

/**
 * Upsert customer_scores + insert score_events audit row in a transaction.
 */
async function applyScore({ customerId, anonymousId, source, eventName, eventId, scoreDelta, eventTime, isBackfill }) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();

    await conn.execute(
      `INSERT INTO customer_scores (customer_id, source, score)
       VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE score = score + VALUES(score), updated_at = NOW()`,
      [customerId, source, scoreDelta],
    );

    await conn.execute(
      `INSERT INTO score_events
         (customer_id, anonymous_id, source, event_name, event_id, score_delta, is_backfill, event_time)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [customerId, anonymousId, source, eventName, eventId ?? null, scoreDelta, isBackfill ? 1 : 0, eventTime],
    );

    await conn.commit();
  } catch (err) {
    await conn.rollback();
    logger.error('Failed to apply score', { component: COMPONENT, customerId, eventName, error: err.message });
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * Fetch all past events for an anonymous_id + source from ClickHouse.
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
