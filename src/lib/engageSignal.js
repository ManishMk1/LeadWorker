/**
 * Engage Signal Dispatcher
 *
 * Sends signals from LeadWorkers → revolt-engage Journey Engine.
 * Fire-and-forget: never blocks the main event processing pipeline.
 * If revolt-engage is down, signals are logged and dropped (not retried).
 *
 * Signals are sent via HTTP POST to revolt-engage's /api/signals endpoint.
 */

import config from '../config/index.js';
import logger from './logger.js';

const COMPONENT = 'engageSignal';

/**
 * Dispatch a signal to the journey engine.
 * Always fire-and-forget — never throws, never blocks.
 *
 * @param {object} params
 * @param {number} params.customerId
 * @param {string} params.signalType    — e.g. 'segment_warm', 'test_ride_booked', 'emi_calculator_used'
 * @param {object} [params.metadata]    — extra context (e.g. ride_date_time for pre_ride)
 */
export function dispatchSignal({ customerId, signalType, metadata = {} }) {
  if (!config.engage.enabled) return;
  if (!customerId || !signalType) return;

  // Fire-and-forget — don't await
  sendSignal({ customerId, signalType, metadata }).catch((err) => {
    logger.warn('Failed to dispatch engage signal', {
      component: COMPONENT, customerId, signalType, error: err.message,
    });
  });
}

async function sendSignal({ customerId, signalType, metadata }) {
  const url = `${config.engage.apiUrl}/api/signals`;

  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ customerId, signalType, metadata }),
    signal: AbortSignal.timeout(5000), // 5s timeout — don't hang
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`${response.status} - ${text}`);
  }

  const data = await response.json();

  logger.debug('Engage signal dispatched', {
    component: COMPONENT, customerId, signalType, action: data.action,
  });
}

export default { dispatchSignal };
