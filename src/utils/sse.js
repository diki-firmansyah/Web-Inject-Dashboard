'use strict';

const sseClients = new Set();

/**
 * Broadcasts a message to all connected SSE clients.
 * @param {string} type - Event type (e.g. 'log', 'dispatch_result')
 * @param {object} payload - Key-value details
 */
function broadcast(type, payload) {
  const data = JSON.stringify({ type, ...payload });
  for (const r of sseClients) {
    try {
      r.write(`data: ${data}\n\n`);
    } catch (err) {
      console.error('[SSE] Broadcast error to a client:', err.message);
    }
  }
}

module.exports = {
  sseClients,
  broadcast
};
