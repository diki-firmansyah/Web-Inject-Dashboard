'use strict';

const { getStatus } = require('../config/db');
const { sseClients } = require('../utils/sse');
const { log, toWIBStr, nowWIB } = require('../utils/logger');

function checkStatus(req, res) {
  const dbStatus = getStatus();
  
  res.json({
    ok: true,
    kafka: dbStatus.kafkaEnabled ? (dbStatus.kafkaConnected ? 'connected' : 'disconnected') : 'disabled',
    mongo: dbStatus.mongoEnabled ? (dbStatus.mongoConnected ? 'connected' : 'disconnected') : 'disabled',
    instagramAccountMongo: dbStatus.instagramAccountMongoConnected ? 'connected' : 'disconnected',
    sseClients: sseClients.size,
    debugMode: process.env.DEBUG_MODE !== 'false',
  });
}

function streamEvents(req, res) {
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
  
  sseClients.add(res);
  const hb = setInterval(() => res.write(': heartbeat\n\n'), 25000);
  
  req.on('close', () => {
    clearInterval(hb);
    sseClients.delete(res);
  });
}

module.exports = {
  checkStatus,
  streamEvents
};
