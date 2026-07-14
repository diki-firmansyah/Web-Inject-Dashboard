'use strict';
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const path = require('path');
const { connectAll } = require('./src/config/db');

const instagramRoutes = require('./src/routes/instagramRoutes');
const tiktokRoutes = require('./src/routes/tiktokRoutes');
const twitterRoutes = require('./src/routes/twitterRoutes');
const youtubeRoutes = require('./src/routes/youtubeRoutes');
const threadsRoutes = require('./src/routes/threadsRoutes');
const linkedinRoutes = require('./src/routes/linkedinRoutes');
const facebookRoutes = require('./src/routes/facebookRoutes');
const batchRoutes = require('./src/routes/batchRoutes');
const statusRoutes = require('./src/routes/statusRoutes');

const PORT = process.env.PORT || 3000;
const publicDir = path.join(__dirname, 'public');
const AUTH_USERNAME = process.env.AUTH_USERNAME || 'admin';
const AUTH_PASSWORD = process.env.AUTH_PASSWORD || 'admin';
const AUTH_COOKIE_NAME = 'webinject_session';
const SESSION_TTL_MS = Number(process.env.AUTH_SESSION_TTL_MS || 8 * 60 * 60 * 1000);
const sessions = new Map();

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: false }));

function parseCookies(req) {
  return String(req.headers.cookie || '')
    .split(';')
    .map(part => part.trim())
    .filter(Boolean)
    .reduce((acc, part) => {
      const index = part.indexOf('=');
      if (index === -1) return acc;
      acc[decodeURIComponent(part.slice(0, index))] = decodeURIComponent(part.slice(index + 1));
      return acc;
    }, {});
}

function getSession(req) {
  const token = parseCookies(req)[AUTH_COOKIE_NAME];
  if (!token) return null;

  const session = sessions.get(token);
  if (!session) return null;

  if (session.expiresAt <= Date.now()) {
    sessions.delete(token);
    return null;
  }

  session.expiresAt = Date.now() + SESSION_TTL_MS;
  return { token, session };
}

function isAuthenticated(req) {
  return !!getSession(req);
}

function setSessionCookie(res, token) {
  const maxAgeSeconds = Math.floor(SESSION_TTL_MS / 1000);
  res.setHeader('Set-Cookie', `${AUTH_COOKIE_NAME}=${encodeURIComponent(token)}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${maxAgeSeconds}`);
}

function clearSessionCookie(res) {
  res.setHeader('Set-Cookie', `${AUTH_COOKIE_NAME}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0`);
}

function requireAuthPage(req, res, next) {
  if (isAuthenticated(req)) return next();
  return res.redirect('/login');
}

function requireAuthApi(req, res, next) {
  if (isAuthenticated(req)) return next();
  return res.status(401).json({ ok: false, error: 'Unauthorized' });
}

app.get('/login', (req, res) => {
  if (isAuthenticated(req)) return res.redirect('/');
  return res.sendFile(path.join(publicDir, 'login.html'));
});

app.post('/login', (req, res) => {
  const username = String(req.body.username || '');
  const password = String(req.body.password || '');

  if (username !== AUTH_USERNAME || password !== AUTH_PASSWORD) {
    return res.redirect('/login?error=1');
  }

  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, {
    username,
    createdAt: Date.now(),
    expiresAt: Date.now() + SESSION_TTL_MS,
  });
  setSessionCookie(res, token);
  return res.redirect('/');
});

app.post('/logout', (req, res) => {
  const active = getSession(req);
  if (active) sessions.delete(active.token);
  clearSessionCookie(res);
  return res.redirect('/login');
});

app.get('/', requireAuthPage, (_req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.get('/index.html', requireAuthPage, (_req, res) => {
  res.sendFile(path.join(publicDir, 'index.html'));
});

app.use(express.static(publicDir, { index: false }));

// Mount routes
app.use('/api', requireAuthApi);
app.use('/api', instagramRoutes);
app.use('/api', tiktokRoutes);
app.use('/api', twitterRoutes);
app.use('/api', youtubeRoutes);
app.use('/api', threadsRoutes);
app.use('/api', linkedinRoutes);
app.use('/api', facebookRoutes);
app.use('/api', batchRoutes);
app.use('/api', statusRoutes);

async function main() {
  // Connect to MongoDB and Kafka on startup
  await connectAll();
  
  app.listen(PORT, () => {
    console.log(`\n╔══════════════════════════════════════════╗`);
    console.log(`║  WebInject Dashboard — Portal            ║`);
    console.log(`║  http://localhost:${PORT}                   ║`);
    console.log(`╚══════════════════════════════════════════╝\n`);
  });
}

main().catch(console.error);
