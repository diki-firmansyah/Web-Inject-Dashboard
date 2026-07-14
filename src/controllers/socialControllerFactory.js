'use strict';

const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');

function createSocialController({ label, fetchPost, dispatchItems }) {
  function targetLabel(target) {
    if (target && typeof target === 'object') return target.url || target.link || JSON.stringify(target);
    return String(target || '');
  }

  async function crawlUrls(urls) {
    const allItems = [];
    const delay = ms => new Promise(r => setTimeout(r, ms));
    let successCount = 0;
    let failCount = 0;
    const failedTargets = [];

    for (const url of urls) {
      try {
        const result = await fetchPost(url);
        if (result.status === 'ok' && result.items.length > 0) {
          successCount++;
          result.items.forEach(item => {
            if (item && typeof item === 'object') {
              Object.defineProperty(item, '__source_url', {
                value: targetLabel(url),
                enumerable: false,
                configurable: true,
              });
            }
          });
          allItems.push(...result.items);
        } else {
          failCount++;
          failedTargets.push(targetLabel(url));
        }
      } catch (e) {
        failCount++;
        failedTargets.push(targetLabel(url));
        log('ERROR', `Unhandled error for ${label} ${targetLabel(url)}: ${e.message}`, { url: targetLabel(url) });
      }

      if (url !== urls[urls.length - 1]) {
        const sleep = 2000 + Math.floor(Math.random() * 2000);
        log('INFO', `  Sleeping ${sleep}ms...`);
        await delay(sleep);
      }
    }

    if (urls.length > 1 || failCount > 0) {
      log('INFO', `${label} SUMMARY: Total: ${urls.length} | Berhasil: ${successCount} | Gagal: ${failCount}`);
      if (failedTargets.length > 0) {
        log('WARN', `${label} target gagal: ${failedTargets.join(', ')}`);
      }
    }

    return { allItems, successCount, failCount, failedTargets };
  }

  async function crawl(req, res) {
    const { urls = [] } = req.body;
    if (!urls.length) return res.status(400).json({ error: 'No URLs provided' });

    log('INFO', `${label} Crawl started - ${urls.length} target(s): ${urls.map(targetLabel).join(', ')}`);
    const { allItems, failedTargets } = await crawlUrls(urls);

    if (allItems.length === 0) {
      log('ERROR', `${label} Crawl gagal - 0 item(s) berhasil diambil.`);
      broadcast('crawl_done', { total: 0 });
      return res.status(400).json({ ok: false, count: 0, items: [], failedTargets, error: `Crawl gagal mengambil data ${label}` });
    }

    log('INFO', `${label} Crawl done - ${allItems.length} item(s) ready for review.`);
    broadcast('crawl_done', { total: allItems.length });
    res.json({ ok: true, count: allItems.length, items: allItems, failedTargets });
  }

  async function send(req, res) {
    const { items = [] } = req.body;
    if (!items.length) return res.status(400).json({ error: 'No items provided' });

    log('INFO', `Sending ${items.length} ${label} item(s) to Kafka/Mongo...`);
    try {
      const results = await dispatchItems(items);
      broadcast('send_done', { total: results.length });
      res.json({ ok: true, results });
    } catch (e) {
      log('ERROR', `${label} Send failed: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  }

  async function crawlAndSend(req, res) {
    const { urls = [] } = req.body;
    if (!urls.length) return res.status(400).json({ error: 'No URLs provided' });

    log('INFO', `${label} Auto crawl+send - ${urls.length} target(s): ${urls.map(targetLabel).join(', ')}`);
    const { allItems, failedTargets } = await crawlUrls(urls);

    if (allItems.length === 0) {
      log('ERROR', `${label} Crawl gagal - 0 item(s) berhasil diambil. Pengiriman dibatalkan.`);
      broadcast('crawl_done', { total: 0 });
      return res.status(400).json({ ok: false, count: 0, failedTargets, error: `Tidak ada item ${label} yang berhasil di-crawl` });
    }

    log('INFO', `${label} Crawl selesai - ${allItems.length} item(s). Langsung kirim ke Kafka/Mongo...`);
    broadcast('crawl_done', { total: allItems.length });

    try {
      const results = await dispatchItems(allItems);
      broadcast('send_done', { total: results.length });
      log('SUCCESS', `${label} Auto-send selesai - ${results.length} item(s)`);
      const failedDispatchTargets = results
        .filter(result => result?.kafka === 'failed' || result?.mongo === 'failed')
        .map(result => ({ url: result.url || result.id || result.shortcode, error: result.kafkaError || result.mongoError || 'Send failed' }));
      res.json({ ok: true, count: allItems.length, failedTargets, failedDispatchTargets, results });
    } catch (e) {
      log('ERROR', `${label} Auto-send failed: ${e.message}`);
      res.status(500).json({ error: e.message });
    }
  }

  return { crawl, send, crawlAndSend };
}

module.exports = { createSocialController };
