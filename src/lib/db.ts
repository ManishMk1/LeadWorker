import mysql from 'mysql2/promise';
import config from '../config/index.js';
import logger from './logger.js';

const pool = mysql.createPool({
  host: config.mysql.host,
  port: config.mysql.port,
  user: config.mysql.user,
  password: config.mysql.password,
  database: config.mysql.database,
  connectionLimit: config.mysql.connectionLimit,
  waitForConnections: true,
  queueLimit: 0,
});

export async function query(sql: string, params?: any[]) {
  const [rows] = await pool.execute(sql, params);
  return rows;
}

export async function getConnection() {
  return pool.getConnection();
}

export async function testConnection() {
  try {
    const connection = await pool.getConnection();
    await connection.ping();
    connection.release();
    logger.info('MySQL connection established successfully');
    return true;
  } catch (error) {
    logger.error('MySQL connection failed:', (error as Error).message);
    throw error;
  }
}

export default pool;
