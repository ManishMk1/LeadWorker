/**
 * Worker Process Entry Point
 *
 * Orchestrates the lifecycle of all registered Kafka workers.
 * Handles startup, graceful shutdown, and uncaught errors.
 */
import logger from './lib/logger.js';
import workers from './workers/index.js';

const SHUTDOWN_TIMEOUT_MS = 15000;
let isShuttingDown = false;

/**
 * Start all registered workers.
 */
async function startAll() {
  logger.info('Starting worker process', {
    component: 'main',
    workerCount: workers.length,
    workers: workers.map((w) => w.name),
    pid: process.pid,
    nodeVersion: process.version,
  });

  const results = await Promise.allSettled(
    workers.map(async (worker) => {
      try {
        await worker.module.start();
        logger.info(`Worker started: ${worker.name}`, { component: 'main' });
      } catch (error) {
        logger.error(`Failed to start worker: ${worker.name}`, {
          component: 'main',
          error: error.message,
          stack: error.stack,
        });
        throw error;
      }
    })
  );

  const failed = results.filter((r) => r.status === 'rejected');
  if (failed.length > 0) {
    logger.error('Some workers failed to start', {
      component: 'main',
      failedCount: failed.length,
      totalCount: workers.length,
    });
    // If all workers failed, exit
    if (failed.length === workers.length) {
      throw new Error('All workers failed to start');
    }
  }

  logger.info('Worker process ready', {
    component: 'main',
    runningWorkers: results.filter((r) => r.status === 'fulfilled').length,
  });
}

/**
 * Gracefully stop all workers with a timeout.
 */
async function shutdownAll(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info(`Received ${signal}, initiating graceful shutdown...`, {
    component: 'main',
    timeoutMs: SHUTDOWN_TIMEOUT_MS,
  });

  // Force exit if shutdown takes too long
  const forceExitTimer = setTimeout(() => {
    logger.error('Shutdown timed out, forcing exit', { component: 'main' });
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);

  forceExitTimer.unref();

  try {
    await Promise.allSettled(
      workers.map(async (worker) => {
        try {
          await worker.module.stop();
          logger.info(`Worker stopped: ${worker.name}`, { component: 'main' });
        } catch (error) {
          logger.error(`Error stopping worker: ${worker.name}`, {
            component: 'main',
            error: error.message,
          });
        }
      })
    );

    logger.info('All workers stopped, exiting', { component: 'main' });
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', {
      component: 'main',
      error: error.message,
    });
    process.exit(1);
  }
}

// --- Signal handlers ---
process.on('SIGINT', () => shutdownAll('SIGINT'));
process.on('SIGTERM', () => shutdownAll('SIGTERM'));

// --- Uncaught error handlers ---
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', {
    component: 'main',
    error: error.message,
    stack: error.stack,
  });
  shutdownAll('uncaughtException');
});

process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled rejection', {
    component: 'main',
    error: reason instanceof Error ? reason.message : String(reason),
    stack: reason instanceof Error ? reason.stack : undefined,
  });
  shutdownAll('unhandledRejection');
});

// --- Start ---
startAll().catch((error) => {
  logger.error('Worker process failed to start', {
    component: 'main',
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});
