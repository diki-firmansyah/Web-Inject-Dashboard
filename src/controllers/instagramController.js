'use strict';

const { getCookies, updateCookies, getHeaderConfig, updateHeaderConfig } = require('../config/state');
const { shortcodeFromUrl, fetchShortcode, dispatchItems } = require('../services/instagramService');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');

function sourceUrlForShortcode(shortcode, urls) {
  const original = (urls || []).find(url => shortcodeFromUrl(String(url || '')) === shortcode);
  return original ? String(original).trim() : `https://www.instagram.com/p/${shortcode}/`;
}

async function crawl(req, res) {
  const { urls = [] } = req.body;
  if (!urls.length) return res.status(400).json({ error: 'No URLs provided' });

  const shortcodes = [...new Set(urls.map(shortcodeFromUrl).filter(Boolean))];
  log('INFO', `⚡ Crawl started — ${shortcodes.length} target(s): ${shortcodes.join(', ')}`);

  const allItems = [];
  const delay    = ms => new Promise(r => setTimeout(r, ms));
  let successCount = 0;
  let failCount = 0;
  let failedTargets = [];

  for (const sc of shortcodes) {
    try {
      const result = await fetchShortcode(sc);
      if (result.status === 'ok' && result.items.length > 0) {
        successCount++;
        allItems.push(...result.items);
      } else {
        failCount++;
        failedTargets.push(sourceUrlForShortcode(sc, urls));
      }
    } catch (e) {
      failCount++;
      failedTargets.push(sourceUrlForShortcode(sc, urls));
      log('ERROR', `Unhandled error for ${sc}: ${e.message}`, { shortcode: sc });
    }
    if (sc !== shortcodes[shortcodes.length - 1]) {
      const sleep = 1000 + Math.floor(Math.random() * 1500);
      log('INFO', `  Sleeping ${sleep}ms...`);
      await delay(sleep);
    }
  }

  // Summary if multiple items or failure occurs
  if (shortcodes.length > 1 || failCount > 0) {
    log('INFO', `📊 SUMMARY: Total: ${shortcodes.length} | Berhasil: ${successCount} | Gagal: ${failCount}`);
    if (failedTargets.length > 0) {
      log('WARN', `❌ Target gagal: ${failedTargets.join(', ')}`);
    }
  }

  if (allItems.length === 0) {
    log('ERROR', `❌ Crawl gagal — 0 item(s) berhasil diambil.`);
    broadcast('crawl_done', { total: 0 });
    return res.status(400).json({ ok: false, count: 0, items: [], failedTargets, error: 'Crawl gagal mengambil data (akun terblokir / rate limit)' });
  }

  log('INFO', `🏁 Crawl done — ${allItems.length} item(s) ready for review.`);
  broadcast('crawl_done', { total: allItems.length });
  res.json({ ok: true, count: allItems.length, items: allItems, failedTargets });
}

async function send(req, res) {
  const { items = [] } = req.body;
  if (!items.length) return res.status(400).json({ error: 'No items provided' });

  log('INFO', `📤 Sending ${items.length} item(s) to Kafka/Mongo...`);
  try {
    const results = await dispatchItems(items);
    broadcast('send_done', { total: results.length });
    res.json({ ok: true, results });
  } catch (e) {
    log('ERROR', `Send failed: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
}

async function crawlAndSend(req, res) {
  const { urls = [] } = req.body;
  if (!urls.length) return res.status(400).json({ error: 'No URLs provided' });

  const shortcodes = [...new Set(urls.map(shortcodeFromUrl).filter(Boolean))];
  log('INFO', `⚡ Auto crawl+send — ${shortcodes.length} target(s): ${shortcodes.join(', ')}`);

  const allItems = [];
  const delay    = ms => new Promise(r => setTimeout(r, ms));
  let successCount = 0;
  let failCount = 0;
  let failedTargets = [];

  for (const sc of shortcodes) {
    try {
      const result = await fetchShortcode(sc);
      if (result.status === 'ok' && result.items.length > 0) {
        successCount++;
        allItems.push(...result.items);
      } else {
        failCount++;
        failedTargets.push(sourceUrlForShortcode(sc, urls));
      }
    } catch (e) {
      failCount++;
      failedTargets.push(sourceUrlForShortcode(sc, urls));
      log('ERROR', `Unhandled error for ${sc}: ${e.message}`, { shortcode: sc });
    }
    if (sc !== shortcodes[shortcodes.length - 1]) {
      const sleep = 1000 + Math.floor(Math.random() * 1500);
      log('INFO', `  Sleeping ${sleep}ms...`);
      await delay(sleep);
    }
  }

  // Summary if multiple items or failure occurs
  if (shortcodes.length > 1 || failCount > 0) {
    log('INFO', `📊 SUMMARY: Total: ${shortcodes.length} | Berhasil: ${successCount} | Gagal: ${failCount}`);
    if (failedTargets.length > 0) {
      log('WARN', `❌ Target gagal: ${failedTargets.join(', ')}`);
    }
  }

  if (allItems.length === 0) {
    log('ERROR', `❌ Crawl gagal — 0 item(s) berhasil diambil. Pengiriman ke Kafka dibatalkan.`);
    broadcast('crawl_done', { total: 0 });
    return res.status(400).json({ ok: false, count: 0, failedTargets, error: 'Tidak ada item yang berhasil di-crawl (akun terblokir / rate limit)' });
  }

  log('INFO', `🏁 Crawl selesai — ${allItems.length} item(s). Langsung kirim ke Kafka/Mongo...`);
  broadcast('crawl_done', { total: allItems.length });

  try {
    const results = await dispatchItems(allItems);
    broadcast('send_done', { total: results.length });
    log('SUCCESS', `✓ Auto-send selesai — ${results.length} item(s)`);
    const failedDispatchTargets = results
      .filter(result => result?.kafka === 'failed' || result?.mongo === 'failed')
      .map(result => ({ url: result.url || result.id || result.shortcode, phase: 'send' }));
    res.json({ ok: true, count: allItems.length, failedTargets, failedDispatchTargets, results });
  } catch (e) {
    log('ERROR', `Auto-send failed: ${e.message}`);
    res.status(500).json({ error: e.message });
  }
}

function getCookiesHandler(_req, res) {
  res.json(getCookies());
}

function updateCookiesHandler(req, res) {
  if (typeof req.body !== 'object' || Array.isArray(req.body)) {
    return res.status(400).json({ error: 'Expected JSON object' });
  }
  const cookies = updateCookies(req.body);
  log('INFO', `Cookies updated (${Object.keys(cookies).length} keys)`);
  res.json({ ok: true, cookies });
}

function getConfigHandler(_req, res) {
  res.json(getHeaderConfig());
}

function updateConfigHandler(req, res) {
  if (typeof req.body !== 'object') {
    return res.status(400).json({ error: 'Expected JSON object' });
  }
  const config = updateHeaderConfig(req.body);
  log('INFO', 'Header config updated');
  res.json({ ok: true, config });
}

module.exports = {
  crawl,
  send,
  crawlAndSend,
  getCookies: getCookiesHandler,
  updateCookies: updateCookiesHandler,
  getConfig: getConfigHandler,
  updateConfig: updateConfigHandler
};
