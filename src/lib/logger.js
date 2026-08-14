import path from 'path';
import { createLogger, format, transports } from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import config from '../config/index.js';

const { combine, timestamp, json, errors, printf, colorize } = format;

const devFormat = printf(({ level, message, timestamp, ...meta }) => {
  const metaStr = Object.keys(meta).length ? ` | ${JSON.stringify(meta)}` : '';
  return `${timestamp} [${level}] ${message}${metaStr}`;
});

const baseFormat = combine(
  timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
  errors({ stack: true }),
  format((info) => {
    info.service = 'revolt-events-consumer';
    info.environment = config.env;
    info.pid = process.pid;
    return info;
  })()
);

const productionTransports = [
  new DailyRotateFile({
    filename: path.join(config.logging.dir, 'error-%DATE%.log'),
    level: 'error',
    datePattern: 'YYYY-MM-DD',
    maxSize: '10m',
    maxFiles: '3d',
    zippedArchive: true,
  }),
  new DailyRotateFile({
    filename: path.join(config.logging.dir, 'combined-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    maxSize: '50m',
    maxFiles: '3d',
    zippedArchive: true,
  }),
];

const developmentTransports = [
  new transports.Console({
    format: combine(colorize(), devFormat),
  }),
];

const logger = createLogger({
  level: config.logging.level,
  format: combine(baseFormat, json()),
  transports:
    config.env === 'production' ? productionTransports : developmentTransports,
  exitOnError: false,
});

if (config.env === 'production') {
  logger.add(
    new transports.Console({
      format: combine(baseFormat, json()),
    })
  );
}

export default logger;
