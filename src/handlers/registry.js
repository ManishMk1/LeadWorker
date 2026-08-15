/**
 * Handler Registry
 *
 * Maps source names → their handler modules.
 * Each handler must export: { SOURCE, isIdentityEvent, extractIdentity, transform }
 *
 * To add a new source:
 *  1. Create src/handlers/<source>/index.js implementing the interface above
 *  2. Import it here and add it to the map — nothing else changes
 */

import * as websiteHandler from './website/index.js';

const handlers = new Map([
  [websiteHandler.SOURCE, websiteHandler],

  // Future sources — just uncomment and implement:
  // [whatsappHandler.SOURCE, whatsappHandler],
  // [lsqHandler.SOURCE,      lsqHandler],
]);

/**
 * Get the handler for a given source name.
 * Returns null if no handler is registered.
 *
 * @param {string} source
 * @returns {object|null}
 */
export function getHandler(source) {
  return handlers.get(source) ?? null;
}

export default { getHandler };
