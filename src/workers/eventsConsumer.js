import { v4 as uuidv4 } from 'uuid';
import config from '../config/index.js';
import logger from '../lib/logger.js';
import { createConsumer } from '../lib/kafka.js';
import { insert, close as closeClickHouse } from '../lib/clickhouse.js';

const WORKER_NAME = 'events-consumer';
const TOPIC = config.kafka.topic;
const CONSUMER_GROUP_ID = config.kafka.consumerGroupId;
const BATCH_SIZE = config.consumer.batchSize;
const BATCH_TIMEOUT_MS = config.consumer.batchTimeoutMs;
const CLICKHOUSE_TABLE = 'revolt_events';

let consumer = null;
let messageBuffer = [];
let batchTimer = null;
let isShuttingDown = false;

/**
 * Transform Kafka payload into revolt_events row.
 * Handles multiple payload formats (website, whatsapp, calling, etc.)
 */
function transformMessage(payload) {
  const eventId = payload.requestId || uuidv4();
  const eventName = payload.event_name || payload.eventName || 'unknown';
  const source = payload.source || 'unknown';
  const customerId = payload.customer_id || payload.contact?.waId || payload.contact?.phoneNumber || '';
  const createdAt = payload.ingestedAt
    ? new Date(payload.ingestedAt).toISOString().replace('T', ' ').slice(0, 19)
    : new Date().toISOString().replace('T', ' ').slice(0, 19);

  const properties = typeof payload.properties === 'object'
    ? JSON.stringify(payload.properties)
    : JSON.stringify({
        messageType: payload.messageType,
        message: payload.message,
        contact: payload.contact,
        metadata: payload.metadata,
      });

  return {
    event_id: eventId,
    event_name: eventName,
    source,
    customer_id: customerId,
    properties,
    created_at: createdAt,
    updated_at: createdAt,
  };
}

/**
 * Flush buffered messages to ClickHouse.
 */
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
      component: WORKER_NAME,
      rowCount: batch.length,
    });
  } catch (error) {
    logger.error('Failed to insert batch into ClickHouse', {
      component: WORKER_NAME,
      rowCount: batch.length,
      error: error.message,
      stack: error.stack,
    });
    // Push failed records back for retry (only if not shutting down)
    if (!isShuttingDown) {
      messageBuffer.unshift(...batch);
    }
  }
}

/**
 * Start batch flush timer.
 */
function startBatchTimer() {
  if (batchTimer) return;
  batchTimer = setTimeout(async () => {
    batchTimer = null;
    await flushToClickHouse();
  }, BATCH_TIMEOUT_MS);
}

/**
 * Start the events consumer worker.
 */
export async function start() {
  logger.info('Starting events consumer worker', {
    component: WORKER_NAME,
    topic: TOPIC,
    groupId: CONSUMER_GROUP_ID,
    batchSize: BATCH_SIZE,
    batchTimeoutMs: BATCH_TIMEOUT_MS,
  });

  consumer = createConsumer(CONSUMER_GROUP_ID);

  consumer.on('consumer.connect', () => {
    logger.info('Consumer connected to Kafka', { component: WORKER_NAME });
  });

  consumer.on('consumer.disconnect', () => {
    logger.warn('Consumer disconnected from Kafka', { component: WORKER_NAME });
  });

  consumer.on('consumer.crash', ({ payload }) => {
    logger.error('Consumer crashed', {
      component: WORKER_NAME,
      error: payload.error?.message,
      groupId: payload.groupId,
    });
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  logger.info(`Subscribed to topic: ${TOPIC}`, { component: WORKER_NAME });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      if (isShuttingDown) return;

      try {
        const rawValue = message.value.toString();
        const payload = JSON.parse(rawValue);
        const row = transformMessage(payload);

        messageBuffer.push(row);

        logger.debug('Message buffered', {
          component: WORKER_NAME,
          eventId: row.event_id,
          eventName: row.event_name,
          source: row.source,
          bufferSize: messageBuffer.length,
        });

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

/**
 * Gracefully stop the events consumer worker.
 */
export async function stop() {
  isShuttingDown = true;

  logger.info('Stopping events consumer worker...', { component: WORKER_NAME });

  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  // Flush remaining messages
  await flushToClickHouse();

  if (consumer) {
    await consumer.disconnect();
    consumer = null;
  }

  await closeClickHouse();

  logger.info('Events consumer worker stopped', { component: WORKER_NAME });
}

export default { start, stop };
