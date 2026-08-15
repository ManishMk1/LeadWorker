/**
 * Website Source Handler
 *
 * Responsible for:
 *  1. Normalising raw website Kafka payloads into a standard EventContext
 *  2. Declaring which events trigger identity resolution (form submissions)
 *  3. Extracting identity fields (phone, email, name) from the payload
 *
 * This is the ONLY file that knows about website-specific payload shapes.
 * Core services (IdentityService, ScoringService) are completely source-agnostic.
 *
 * To add a new source (e.g. whatsapp):
 *   - Create src/handlers/whatsapp/index.js
 *   - Implement the same interface: { SOURCE, isIdentityEvent, extractIdentity, transform }
 *   - Register it in src/handlers/registry.js
 */

import { v4 as uuidv4 } from 'uuid';

export const SOURCE = 'website';

/**
 * Events that should trigger customer creation / identity linking.
 * Anything NOT in this set is a passive tracking event.
 */
const IDENTITY_EVENTS = new Set([
  'test_ride_booking',
  'book_bike',
  'login',
  'form_submitted',
]);

/**
 * Returns true if this event should trigger identity resolution.
 * @param {string} eventName
 */
export function isIdentityEvent(eventName) {
  return IDENTITY_EVENTS.has(eventName);
}

/**
 * Extract identity fields from a website payload.
 * Returns null if there's nothing useful to identify with.
 *
 * @param {object} payload  Raw Kafka payload
 * @param {string} eventName
 * @returns {{ phone?: string, email?: string, name?: string } | null}
 */
export function extractIdentity(payload, _eventName) {
  const props = payload.properties ?? {};

  const phone = normalisePhone(
    props.phone       ||
    props.mobile      ||
    props.phoneNumber ||
    payload.phone     ||
    null,
  );

  const email = normaliseEmail(
    props.email   ||
    payload.email ||
    null,
  );

  const name =
    props.name      ||
    props.full_name ||
    props.firstName ||
    payload.name    ||
    null;

  if (!phone && !email) return null;

  return { phone, email, name };
}

/**
 * Transform a raw website Kafka payload into the standard row shape
 * that gets written to ClickHouse.
 *
 * @param {object} payload  Raw Kafka message value (parsed JSON)
 * @returns {object}  ClickHouse row
 */
export function transform(payload) {
  const eventId   = payload.requestId || uuidv4();
  const eventName = payload.event_name || payload.eventName || 'unknown';

  const anonymousId =
    payload.customer_id        ||
    payload.anonymous_id       ||
    payload.anonymousId        ||
    '';

  const createdAt = payload.ingestedAt
    ? new Date(payload.ingestedAt).toISOString().replace('T', ' ').slice(0, 19)
    : new Date().toISOString().replace('T', ' ').slice(0, 19);

  const properties =
    typeof payload.properties === 'object'
      ? JSON.stringify(payload.properties)
      : JSON.stringify({
          messageType: payload.messageType,
          message:     payload.message,
          contact:     payload.contact,
          metadata:    payload.metadata,
        });

  return {
    event_id:     eventId,
    event_name:   eventName,
    source:       SOURCE,
    anonymous_id: anonymousId,
    properties,
    created_at:   createdAt,
    updated_at:   createdAt,
  };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function normalisePhone(raw) {
  if (!raw) return null;
  // Strip everything except digits and leading +
  const cleaned = String(raw).replace(/[^\d+]/g, '');
  return cleaned.length >= 7 ? cleaned : null;
}

function normaliseEmail(raw) {
  if (!raw) return null;
  const cleaned = String(raw).trim().toLowerCase();
  return cleaned.includes('@') ? cleaned : null;
}
