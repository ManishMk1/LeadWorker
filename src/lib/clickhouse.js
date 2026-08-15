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
 * Run a SELECT query and return rows as plain objects.
 * Uses ClickHouse named parameters: {paramName: Type}
 *
 * @param {string} sql   - Query string with named params e.g. {anonymousId: String}
 * @param {object} [params] - { paramName: value }
 * @returns {Promise<object[]>}
 */
export async function queryClickHouse(sql, params = {}) {
  const ch = getClient();
  const startTime = Date.now();

  try {
    const resultSet = await ch.query({
      query: sql,
      query_params: params,
      format: 'JSONEachRow',
    });

    const rows = await resultSet.json();
    const duration = Date.now() - startTime;

    logger.debug('ClickHouse query successful', {
      component: 'clickhouse',
      rowCount: rows.length,
      durationMs: duration,
    });

    return rows;
  } catch (error) {
    const duration = Date.now() - startTime;
    logger.error('ClickHouse query failed', {
      component: 'clickhouse',
      durationMs: duration,
      error: error.message,
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

export default { getClient, insert, queryClickHouse, ping, close };
