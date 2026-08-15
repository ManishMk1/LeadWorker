/**
 * MySQL connection pool — singleton using mysql2/promise.
 */

import mysql from 'mysql2/promise';
import config from '../config/index.js';
import logger from './logger.js';

const pool = mysql.createPool({
  host:             config.mysql.host,
  port:             config.mysql.port,
  user:             config.mysql.user,
  password:         config.mysql.password,
  database:         config.mysql.database,
  connectionLimit:  config.mysql.connectionLimit,
  waitForConnections: true,
  queueLimit:       0,
  timezone:         'Z', // store/read all datetimes as UTC
});

/**
 * Run a query and return rows.
 * @param {string} sql
 * @param {any[]} [params]
 */
export async function query(sql, params) {
  const [rows] = await pool.execute(sql, params);
  return rows;
}

/**
 * Get a raw connection from the pool (for transactions).
 * Caller must call connection.release() when done.
 */
export async function getConnection() {
  return pool.getConnection();
}

/**
 * Verify the pool can reach MySQL.
 */
export async function testConnection() {
  try {
    const connection = await pool.getConnection();
    await connection.ping();
    connection.release();
    logger.info('MySQL connection established successfully', { component: 'db' });
    return true;
  } catch (error) {
    logger.error('MySQL connection failed', { component: 'db', error: error.message });
    throw error;
  }
}

export default pool;
