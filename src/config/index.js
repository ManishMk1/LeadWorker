import 'dotenv/config';

const config = {
  env: process.env.NODE_ENV || 'development',

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || 'revolt-events-consumer',
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map((b) => b.trim()),
    topic: process.env.KAFKA_TOPIC || 'webevents.v1.json',
    consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'revolt-events-consumer-group',
    ssl: {
      rejectUnauthorized: process.env.KAFKA_SSL_REJECT_UNAUTHORIZED === 'true',
      caFile: process.env.KAFKA_SSL_CA_FILE || 'ca.crt',
      keyFile: process.env.KAFKA_SSL_KEY_FILE || 'client.key',
      certFile: process.env.KAFKA_SSL_CERT_FILE || 'signed-certificate-form-acm.pem',
    },
  },

  mysql: {
    host: process.env.MYSQL_HOST || 'localhost',
    port: parseInt(process.env.MYSQL_PORT, 10) || 3306,
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    database: process.env.MYSQL_DATABASE || 'leads',
    connectionLimit: parseInt(process.env.MYSQL_CONNECTION_LIMIT, 10) || 10,
  },

  clickhouse: {
    url: process.env.CLICKHOUSE_URL || 'http://localhost:8123',
    database: process.env.CLICKHOUSE_DATABASE || 'analytics',
    username: process.env.CLICKHOUSE_USERNAME || 'default',
    password: process.env.CLICKHOUSE_PASSWORD || '',
    requestTimeout: parseInt(process.env.CLICKHOUSE_REQUEST_TIMEOUT, 10) || 30000,
    maxOpenConnections: parseInt(process.env.CLICKHOUSE_MAX_CONNECTIONS, 10) || 10,
  },

  consumer: {
    batchSize: parseInt(process.env.BATCH_SIZE, 10) || 50,
    batchTimeoutMs: parseInt(process.env.BATCH_TIMEOUT_MS, 10) || 5000,
  },

  logging: {
    level: process.env.LOG_LEVEL || 'info',
    dir: process.env.LOG_DIR || 'logs',
  },
};

export default config;
