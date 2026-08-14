import { Kafka, logLevel } from 'kafkajs';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import config from '../config/index.js';
import logger from './logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CERT_DIR = path.resolve(__dirname, '../certificates');

let kafkaInstance = null;

/**
 * Get or create the Kafka client (singleton).
 * Reusable across producers, consumers, and admin operations.
 */
export function getKafkaClient() {
  if (kafkaInstance) return kafkaInstance;

  logger.info('Initializing Kafka client', {
    component: 'kafka',
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
  });

  kafkaInstance = new Kafka({
    clientId: config.kafka.clientId,
    brokers: config.kafka.brokers,
    ssl: {
      rejectUnauthorized: config.kafka.ssl.rejectUnauthorized,
      ca: [fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.caFile), 'utf-8')],
      key: fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.keyFile), 'utf-8'),
      cert: fs.readFileSync(path.join(CERT_DIR, config.kafka.ssl.certFile), 'utf-8'),
    },
    logLevel: logLevel.INFO,
    retry: {
      initialRetryTime: 300,
      retries: 10,
      maxRetryTime: 30000,
      factor: 2,
    },
  });

  return kafkaInstance;
}

/**
 * Create a consumer instance for the given group ID.
 * @param {string} groupId - Consumer group ID
 * @param {object} [options] - Additional consumer options
 * @returns {import('kafkajs').Consumer}
 */
export function createConsumer(groupId, options = {}) {
  const kafka = getKafkaClient();

  return kafka.consumer({
    groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576, // 1MB
    ...options,
  });
}

/**
 * Create a producer instance.
 * @param {object} [options] - Additional producer options
 * @returns {import('kafkajs').Producer}
 */
export function createProducer(options = {}) {
  const kafka = getKafkaClient();

  return kafka.producer({
    allowAutoTopicCreation: false,
    transactionTimeout: 30000,
    ...options,
  });
}

/**
 * Create an admin client for topic management and health checks.
 * @returns {import('kafkajs').Admin}
 */
export function createAdmin() {
  const kafka = getKafkaClient();
  return kafka.admin();
}

/**
 * Health check — verifies connectivity by listing topics.
 * @returns {Promise<{success: boolean, durationMs: number, error?: string}>}
 */
export async function healthCheck() {
  const startTime = Date.now();

  try {
    const admin = createAdmin();
    await admin.connect();
    await admin.listTopics();
    await admin.disconnect();

    const duration = Date.now() - startTime;
    logger.debug('Kafka health check passed', { component: 'kafka', durationMs: duration });
    return { success: true, durationMs: duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    logger.error('Kafka health check failed', {
      component: 'kafka',
      durationMs: duration,
      error: error.message,
    });
    return { success: false, durationMs: duration, error: error.message };
  }
}

export default {
  getKafkaClient,
  createConsumer,
  createProducer,
  createAdmin,
  healthCheck,
};
