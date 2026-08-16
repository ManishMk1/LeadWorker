/**
 * Events Consumer Worker
 *
 * Reads events from Kafka, fans them out through the correct source handler,
 * runs identity resolution and scoring, then batches raw events to ClickHouse.
 *
 * Per-event logic (in order):
 *  1. Route to the correct source handler (website / whatsapp / lsq / …)
 *  2. Transform raw payload → normalised ClickHouse row
 *  3. If it's a form-submission event → run IdentityService.identifyVisitor
 *     (creates/finds customer, links anonymous_id, fires backfill on first link)
 *  4. If it's a passive event → check Redis/MySQL for a known customer_id
 *     → if known: score the event
 *     → if anonymous: skip scoring (do not process)
 *  5. Buffer the ClickHouse row and flush in batches
 */

import config from '../config/index.js';
import logger from '../lib/logger.js';
import { createConsumer } from '../lib/kafka.js';
import { insert, close as closeClickHouse } from '../lib/clickhouse.js';
import { close as closeRedis } from '../lib/redis.js';
import { getHandler } from '../handlers/registry.js';
import { resolveCustomerId, identifyVisitor } from '../services/IdentityService.js';
import { scoreEvent } from '../services/ScoringService.js';
import { advance as advanceLifecycle } from '../services/LifecycleService.js';
import { dispatchSignal } from '../lib/engageSignal.js';

const WORKER_NAME      = 'events-consumer';
const TOPIC            = config.kafka.topic;
const CONSUMER_GROUP_ID = config.kafka.consumerGroupId;
const BATCH_SIZE       = config.consumer.batchSize;
const BATCH_TIMEOUT_MS = config.consumer.batchTimeoutMs;
const CLICKHOUSE_TABLE = 'rev_events';

let consumer      = null;
let messageBuffer = [];
let batchTimer    = null;
let isShuttingDown = false;

// ---------------------------------------------------------------------------
// Core per-message processing
// ---------------------------------------------------------------------------

/**
 * Process a single Kafka message end-to-end.
 *
 * @param {object} payload  Parsed JSON from Kafka message value
 */
async function processMessage(payload) {
  const source = payload.source || 'unknown';

  // 1. Get the handler for this source
  const handler = getHandler(source);

  if (!handler) {
    logger.warn('No handler registered for source — skipping identity/scoring, writing to ClickHouse anyway', {
      component: WORKER_NAME, source,
    });
    // Still write to ClickHouse so we don't lose the event
    messageBuffer.push(buildFallbackRow(payload, source));
    return;
  }

  // 2. Transform to ClickHouse row shape
  const row = handler.transform(payload);
  const { event_name: eventName, anonymous_id: anonymousId, event_id: eventId, created_at: createdAt } = row;

  if (!anonymousId) {
    logger.warn('Event has no anonymous_id — skipping identity/scoring', {
      component: WORKER_NAME, source, eventName,
    });
    messageBuffer.push(row);
    return;
  }

  const eventTime = new Date(createdAt);

  // 3. Identity resolution (form submission events)
  if (handler.isIdentityEvent(eventName)) {
    const identity = handler.extractIdentity(payload, eventName);

    if (identity) {
      try {
        const result = await identifyVisitor({
          source,
          anonymousId,
          ...identity,
        });

        if (result) {
          // Score the form-submission event itself
          await scoreEvent({
            customerId: result.customerId,
            anonymousId,
            source,
            eventName,
            eventId,
            eventTime,
          });

          // Advance lifecycle stage (fire-and-forget — don't block ClickHouse write)
          advanceLifecycle({ customerId: result.customerId, source, eventName, eventTime }).catch((err) => {
            logger.error('Lifecycle advance failed', { component: WORKER_NAME, eventName, error: err.message });
          });

          // Dispatch signal to revolt-engage (covers identity events like test_ride_booking, book_bike)
          dispatchSignal({ customerId: result.customerId, signalType: eventName });
        }
      } catch (err) {
        logger.error('Identity resolution failed', {
          component: WORKER_NAME, source, eventName, anonymousId, error: err.message,
        });
        // Don't throw — we still want the event written to ClickHouse
      }
    } else {
      logger.debug('Identity event but no identity fields found — skipping identity resolution', {
        component: WORKER_NAME, source, eventName, anonymousId,
      });
    }
  } else {
    // 4. Passive event — check if visitor is known, score if so
    try {
      const customerId = await resolveCustomerId(source, anonymousId);

      if (customerId !== null) {
        await scoreEvent({ customerId, anonymousId, source, eventName, eventId, eventTime });

        // Advance lifecycle for known customers (covers non-identity lifecycle events)
        advanceLifecycle({ customerId, source, eventName, eventTime }).catch((err) => {
          logger.error('Lifecycle advance failed', { component: WORKER_NAME, eventName, error: err.message });
        });

        // Dispatch behavioural signal to revolt-engage (fires steps early in running journeys)
        dispatchSignal({ customerId, signalType: eventName });
      } else {
        logger.debug('Anonymous visitor — skipping score', {
          component: WORKER_NAME, source, eventName, anonymousId,
        });
      }
    } catch (err) {
      logger.error('Scoring failed', {
        component: WORKER_NAME, source, eventName, anonymousId, error: err.message,
      });
    }
  }

  // 5. Always buffer for ClickHouse
  messageBuffer.push(row);
}

// ---------------------------------------------------------------------------
// ClickHouse batch flushing
// ---------------------------------------------------------------------------

async function flushToClickHouse() {
  if (messageBuffer.length === 0) return;

  const batch = [...messageBuffer];
  messageBuffer = [];

  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  try {
    await insert(CLICKHOUSE_TABLE, batch);
    logger.info('Batch inserted into ClickHouse', {
      component: WORKER_NAME, rowCount: batch.length,
    });
  } catch (error) {
    logger.error('Failed to insert batch into ClickHouse', {
      component: WORKER_NAME, rowCount: batch.length, error: error.message,
    });
    if (!isShuttingDown) {
      messageBuffer.unshift(...batch); // re-queue for retry
    }
  }
}

function startBatchTimer() {
  if (batchTimer) return;
  batchTimer = setTimeout(async () => {
    batchTimer = null;
    await flushToClickHouse();
  }, BATCH_TIMEOUT_MS);
}

// ---------------------------------------------------------------------------
// Fallback row for unhandled sources
// ---------------------------------------------------------------------------

function buildFallbackRow(payload, source) {
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  return {
    event_id:     payload.requestId || crypto.randomUUID(),
    event_name:   payload.event_name || payload.eventName || 'unknown',
    source,
    anonymous_id: payload.customer_id || payload.anonymous_id || '',
    properties:   JSON.stringify(payload.properties ?? {}),
    created_at:   now,
    updated_at:   now,
  };
}

// ---------------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------------

export async function start() {
  logger.info('Starting events consumer worker', {
    component: WORKER_NAME,
    topic: TOPIC,
    groupId: CONSUMER_GROUP_ID,
    batchSize: BATCH_SIZE,
    batchTimeoutMs: BATCH_TIMEOUT_MS,
  });

  consumer = createConsumer(CONSUMER_GROUP_ID);

  consumer.on('consumer.connect',    () => logger.info('Consumer connected',    { component: WORKER_NAME }));
  consumer.on('consumer.disconnect', () => logger.warn('Consumer disconnected', { component: WORKER_NAME }));
  consumer.on('consumer.crash', ({ payload }) =>
    logger.error('Consumer crashed', { component: WORKER_NAME, error: payload.error?.message }),
  );

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  logger.info(`Subscribed to topic: ${TOPIC}`, { component: WORKER_NAME });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      if (isShuttingDown) return;

      try {
        const payload = JSON.parse(message.value.toString());
        await processMessage(payload);

        if (messageBuffer.length >= BATCH_SIZE) {
          await flushToClickHouse();
        } else {
          startBatchTimer();
        }
      } catch (error) {
        logger.error('Error processing message', {
          component: WORKER_NAME,
          partition,
          offset: message.offset,
          error: error.message,
          stack: error.stack,
        });
      }
    },
  });

  logger.info('Events consumer worker is running', { component: WORKER_NAME });
}

export async function stop() {
  isShuttingDown = true;

  logger.info('Stopping events consumer worker...', { component: WORKER_NAME });

  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  await flushToClickHouse();

  if (consumer) {
    await consumer.disconnect();
    consumer = null;
  }

  await closeClickHouse();
  await closeRedis();

  logger.info('Events consumer worker stopped', { component: WORKER_NAME });
}

export default { start, stop };
