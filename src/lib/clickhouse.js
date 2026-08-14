import { createClient } from '@clickhouse/client';
import config from '../config/index.js';
import logger from './logger.js';

let client = null;

/**
 * Get or create the ClickHouse client (singleton).
 */
export function getClient() {
  if (client) return client;

  logger.info('Initializing ClickHouse client', {
    component: 'clickhouse',
    url: config.clickhouse.url,
    database: config.clickhouse.database,
  });

  client = createClient({
    url: config.clickhouse.url,
    database: config.clickhouse.database,
    username: config.clickhouse.username,
    password: config.clickhouse.password,
    request_timeout: config.clickhouse.requestTimeout,
    max_open_connections: config.clickhouse.maxOpenConnections,
    clickhouse_settings: {
      async_insert: 1,
      wait_for_async_insert: 1,
    },
    compression: {
      request: true,
      response: true,
    },
  });

  return client;
}

/**
 * Insert rows into a table.
 */
export async function insert(table, values) {
  const ch = getClient();
  const startTime = Date.now();

  try {
    await ch.insert({
      table,
      values,
      format: 'JSONEachRow',
    });

    const duration = Date.now() - startTime;
    logger.info('ClickHouse insert successful', {
      component: 'clickhouse',
      table,
      rowCount: values.length,
      durationMs: duration,
    });

    return { success: true, rowCount: values.length, durationMs: duration };
  } catch (error) {
    const duration = Date.now() - startTime;
    logger.error('ClickHouse insert failed', {
      component: 'clickhouse',
      table,
      rowCount: values.length,
      durationMs: duration,
      error: error.message,
      stack: error.stack,
    });
    throw error;
  }
}

/**
 * Health check.
 */
export async function ping() {
  const ch = getClient();
  try {
    const result = await ch.ping();
    return { success: result.success };
  } catch (error) {
    return { success: false, error: error.message };
  }
}

/**
 * Close connection.
 */
export async function close() {
  if (client) {
    logger.info('Closing ClickHouse connection', { component: 'clickhouse' });
    await client.close();
    client = null;
  }
}

export default { getClient, insert, ping, close };
