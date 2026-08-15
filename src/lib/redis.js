/**
 * Redis client — singleton using ioredis.
 *
 * What we cache:
 *  - anon:{source}:{anonymous_id}  → customer_id as string, or "null" if anonymous
 *    TTL: REDIS_ANON_TTL_SECONDS (default 24 h)
 *    This is the hot-path cache. Every event hits this before touching MySQL.
 *
 *  - score_config:{source}         → JSON map { event_name: score }
 *    TTL: REDIS_SCORE_CONFIG_TTL_SECONDS (default 5 min)
 *    Avoids repeated DB reads for the score config table.
 */

import Redis from 'ioredis';
import config from '../config/index.js';
import logger from './logger.js';

let client = null;

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

export function getClient() {
  if (client) return client;

  logger.info('Initializing Redis client', { component: 'redis', url: config.redis.url });

  client = new Redis(config.redis.url, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    lazyConnect: false,
    retryStrategy(times) {
      if (times > 5) return null; // stop retrying after 5 attempts
      return Math.min(times * 200, 2000);
    },
  });

  client.on('connect',   () => logger.info('Redis connected',    { component: 'redis' }));
  client.on('ready',     () => logger.info('Redis ready',        { component: 'redis' }));
  client.on('error',  (e) => logger.error('Redis error',         { component: 'redis', error: e.message }));
  client.on('close',     () => logger.warn('Redis connection closed', { component: 'redis' }));
  client.on('reconnecting', () => logger.info('Redis reconnecting', { component: 'redis' }));

  return client;
}

// ---------------------------------------------------------------------------
// Anonymous profile cache
// anon:{source}:{anonymous_id} → "customerId" | "null"
// ---------------------------------------------------------------------------

const ANON_TTL = () => config.redis.anonTtlSeconds;

/**
 * Get cached customer_id for an anonymous_id.
 * Returns:
 *   null      → cache miss (go to MySQL)
 *   "null"    → cached as anonymous (no customer linked)
 *   number    → customer_id
 */
export async function getCachedAnonymousProfile(source, anonymousId) {
  try {
    const val = await getClient().get(`anon:${source}:${anonymousId}`);
    if (val === null) return undefined; // cache miss
    if (val === 'null') return null;    // known anonymous
    return parseInt(val, 10);           // known customer
  } catch (err) {
    logger.warn('Redis GET failed — falling back to MySQL', { component: 'redis', error: err.message });
    return undefined; // treat as cache miss
  }
}

/**
 * Cache the customer_id (or null) for an anonymous_id.
 * @param {string} source
 * @param {string} anonymousId
 * @param {number|null} customerId
 */
export async function setCachedAnonymousProfile(source, anonymousId, customerId) {
  try {
    const val = customerId == null ? 'null' : String(customerId);
    await getClient().setex(`anon:${source}:${anonymousId}`, ANON_TTL(), val);
  } catch (err) {
    logger.warn('Redis SET failed — continuing without cache', { component: 'redis', error: err.message });
  }
}

/**
 * Invalidate the anonymous profile cache entry.
 * Call this when identity is resolved so the next event re-reads from MySQL.
 */
export async function invalidateCachedAnonymousProfile(source, anonymousId) {
  try {
    await getClient().del(`anon:${source}:${anonymousId}`);
  } catch (err) {
    logger.warn('Redis DEL failed', { component: 'redis', error: err.message });
  }
}

// ---------------------------------------------------------------------------
// Score config cache
// score_config:{source} → JSON string of { event_name: score }
// ---------------------------------------------------------------------------

const SCORE_CONFIG_TTL = () => config.redis.scoreConfigTtlSeconds;

export async function getCachedScoreConfig(source) {
  try {
    const val = await getClient().get(`score_config:${source}`);
    if (!val) return null;
    return JSON.parse(val);
  } catch (err) {
    logger.warn('Redis GET score_config failed', { component: 'redis', error: err.message });
    return null;
  }
}

export async function setCachedScoreConfig(source, scoreMap) {
  try {
    await getClient().setex(`score_config:${source}`, SCORE_CONFIG_TTL(), JSON.stringify(scoreMap));
  } catch (err) {
    logger.warn('Redis SET score_config failed', { component: 'redis', error: err.message });
  }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

export async function close() {
  if (client) {
    logger.info('Closing Redis connection', { component: 'redis' });
    await client.quit();
    client = null;
  }
}

export default { getClient, getCachedAnonymousProfile, setCachedAnonymousProfile, invalidateCachedAnonymousProfile, getCachedScoreConfig, setCachedScoreConfig, close };
