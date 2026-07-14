'use strict';

const { broadcast } = require('./sse');

function nowWIB() {
  return new Date(Date.now() + 7 * 3600_000);
}

function toWIBStr(d) {
  return d.toISOString().replace('T', ' ').slice(0, 19);
}

/**
 * Standardized logger that outputs to console and broadcasts to SSE clients.
 * @param {string} level - Log level ('INFO', 'WARN', 'ERROR', 'SUCCESS')
 * @param {string} msg - The log message text
 * @param {object} meta - Optional metadata to broadcast along with the message (e.g. shortcode)
 */
function log(level, msg, meta = {}) {
  const ts = toWIBStr(nowWIB());
  console.log(`[${ts}] [${level}] ${msg}`);
  broadcast('log', { level, msg, ts, ...meta });
}

module.exports = {
  log,
  nowWIB,
  toWIBStr
};
