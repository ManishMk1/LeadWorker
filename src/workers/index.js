/**
 * Worker Registry
 *
 * Central registry for all Kafka consumer workers.
 * Add new workers here as the system grows.
 */
import eventsConsumer from './eventsConsumer.js';
import decayWorker from './decayWorker.js';

/**
 * All registered workers.
 * Each worker must implement: { start(), stop() }
 */
const workers = [
  { name: 'events-consumer', module: eventsConsumer },
  { name: 'decay-worker',    module: decayWorker    },
  // Add more workers here as needed:
  // { name: 'notifications-consumer', module: notificationsConsumer },
];

export default workers;
