/**
 * Worker Registry
 *
 * Central registry for all Kafka consumer workers.
 * Add new workers here as the system grows.
 */
import eventsConsumer from './eventsConsumer.js';

/**
 * All registered workers.
 * Each worker must implement: { start(), stop() }
 */
const workers = [
  { name: 'events-consumer', module: eventsConsumer },
  // Add more workers here as needed:
  // { name: 'notifications-consumer', module: notificationsConsumer },
  // { name: 'analytics-consumer', module: analyticsConsumer },
];

export default workers;
