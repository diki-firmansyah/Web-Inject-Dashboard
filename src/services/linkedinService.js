'use strict';

const axios = require('axios');
const cheerio = require('cheerio');
const { chromium } = require('playwright');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getKafkaProducer, produceMessage } = require('../config/db');

const LINKEDIN_TOPIC = process.env.KAFKA_TOPIC_LINKEDIN || 'linkedin_post';
const LINKEDIN_TIMEOUT_MS = Number(process.env.LINKEDIN_REQUEST_TIMEOUT_MS || 30000);
const LINKEDIN_DISPATCH_TIMEOUT_MS = Number(process.env.LINKEDIN_DISPATCH_TIMEOUT_MS || 30000);
const LINKEDIN_RENDER_WAIT_MS = Number(process.env.LINKEDIN_RENDER_WAIT_MS || 5000);
const LINKEDIN_HEADLESS = process.env.LINKEDIN_HEADLESS !== 'false';
const LINKEDIN_CRAWLER_AUTHOR = process.env.LINKEDIN_CRAWLER_AUTHOR || 'donell';

function withTimeout(promise, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      setTimeout(() => reject(new Error(`${label} timeout after ${LINKEDIN_DISPATCH_TIMEOUT_MS}ms`)), LINKEDIN_DISPATCH_TIMEOUT_MS);
    }),
  ]);
}

function cleanAscii(value) {
  if (value === undefined || value === null) return null;
  return String(value).replace(/[^\x00-\x7F]/g, '').trim() || null;
}

function getPublicIp() {
  return axios
    .get('https://api.ipify.org?format=json', { timeout: 5000 })
    .then(response => response.data && response.data.ip ? response.data.ip : 'Unable to determine IP')
    .catch(() => 'Unable to determine IP');
}

function findJsonLd($) {
  const candidates = [];
  $('script[type="application/ld+json"]').each((_, el) => {
    const raw = $(el).text();
    if (!raw) return;

    try {
      const parsed = JSON.parse(raw);
      candidates.push(parsed);
    } catch (_) {
      // Ignore malformed JSON-LD blocks.
    }
  });

  return candidates.find(item => item && (item.articleBody || item.datePublished || item.author)) || {};
}

function extractPostId($, urls) {
  const redirectUrl = $('input[name="session_redirect"]').attr('value') || '';
  const candidates = [redirectUrl, ...urls];

  for (const candidate of candidates) {
    const activityMatch = String(candidate).match(/activity[-:_](\d+)/i);
    if (activityMatch) return activityMatch[1];

    const trailingMatch = String(candidate).match(/-(\d{12,})(?:[-/?#]|$)/);
    if (trailingMatch) return trailingMatch[1];

    const urnMatch = String(candidate).match(/urn:li:activity:(\d+)/i);
    if (urnMatch) return urnMatch[1];
  }

  return null;
}

function extractHashtags(content) {
  if (!content) return null;
  const hashtags = String(content).match(/#\w+/g);
  return hashtags && hashtags.length ? hashtags : null;
}

function parseCount(value) {
  if (value === undefined || value === null || value === '') return 0;
  const text = String(value).trim();
  if (!text) return 0;

  const normalized = text
    .replace(/,/g, '')
    .replace(/\./g, '')
    .match(/\d+/);

  return normalized ? normalized[0] : text;
}

function convertDurationToDatetime(durationStr) {
  if (!durationStr) return { datetime: null, datetimeMs: null };

  const now = Date.now();
  const text = String(durationStr).toLowerCase().replace(/edited|diedit/g, '').trim();
  const match = text.match(/(\d+)\s*(y|yr|year|years|w|week|weeks|mgg|d|day|days|h|hr|hour|hours|jam|m|min|mnt|minute|minutes|mo|month|months|bln)/);

  if (!match) return { datetime: null, datetimeMs: null };

  const amount = Number(match[1]);
  const unit = match[2];
  let durationMs = 0;

  if (['y', 'yr', 'year', 'years'].includes(unit)) durationMs = amount * 365 * 24 * 60 * 60 * 1000;
  else if (['w', 'week', 'weeks', 'mgg'].includes(unit)) durationMs = amount * 7 * 24 * 60 * 60 * 1000;
  else if (['d', 'day', 'days'].includes(unit)) durationMs = amount * 24 * 60 * 60 * 1000;
  else if (['h', 'hr', 'hour', 'hours', 'jam'].includes(unit)) durationMs = amount * 60 * 60 * 1000;
  else if (['m', 'min', 'mnt', 'minute', 'minutes'].includes(unit)) durationMs = amount * 60 * 1000;
  else if (['mo', 'month', 'months', 'bln'].includes(unit)) durationMs = amount * 30 * 24 * 60 * 60 * 1000;

  const date = new Date(now - durationMs);
  return {
    datetime: formatDateTime(date, true),
    datetimeMs: date.getTime(),
  };
}

function parsePublishedAt(value) {
  if (!value) return { datetime: null, datetimeMs: null };

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return convertDurationToDatetime(value);

  return {
    datetime: formatDateTime(date, false),
    datetimeMs: date.getTime(),
  };
}

function parsePostTime(postTimeStr, datePublished) {
  const relativeTime = convertDurationToDatetime(postTimeStr);
  if (relativeTime.datetimeMs) return relativeTime;

  const absoluteTime = parsePublishedAt(datePublished || postTimeStr);
  if (absoluteTime.datetimeMs) return absoluteTime;

  return { datetime: null, datetimeMs: null };
}

function formatDateTime(date, includeMicroseconds) {
  const pad = number => String(number).padStart(2, '0');
  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1);
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());

  if (!includeMicroseconds) return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${String(date.getMilliseconds()).padStart(3, '0')}000`;
}

async function renderLinkedinPage(url) {
  let browser;

  try {
    browser = await chromium.launch({
      headless: LINKEDIN_HEADLESS,
      args: [
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--ignore-certificate-errors',
      ],
    });

    const context = await browser.newContext({
      userAgent: process.env.LINKEDIN_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0',
      locale: 'en-US',
      viewport: { width: 1024, height: 800 },
      ignoreHTTPSErrors: true,
    });
    const page = await context.newPage();

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: LINKEDIN_TIMEOUT_MS });
    try {
      await page.waitForLoadState('networkidle', { timeout: Math.min(LINKEDIN_TIMEOUT_MS, 30000) });
    } catch (_) {
      log('WARN', `LinkedIn networkidle timeout for ${url}, continuing with rendered DOM.`);
    }
    await page.waitForTimeout(LINKEDIN_RENDER_WAIT_MS);

    const renderedMeta = await page.evaluate(() => {
      const text = selector => {
        const node = document.querySelector(selector);
        return node && node.textContent ? node.textContent.trim() : null;
      };
      const attr = (selector, name) => {
        const node = document.querySelector(selector);
        return node ? node.getAttribute(name) : null;
      };

      const timeNode = document.querySelector('[datetime]') || document.querySelector('time');
      const postTimeStr = timeNode
        ? (timeNode.getAttribute('datetime') || timeNode.textContent || '').replace(/ Edited| Diedit/g, '').trim()
        : null;

      return {
        reactionCount: text('span[data-test-id="social-actions__reaction-count"]'),
        commentCount: attr('[data-num-comments]', 'data-num-comments'),
        avatar: attr('article img', 'src') || attr('meta[property="og:image"]', 'content'),
        postTimeStr,
      };
    });

    const html = await page.content();
    const finalUrl = page.url();
    await browser.close();

    return { html, finalUrl, renderedMeta };
  } catch (e) {
    if (browser) {
      try {
        await browser.close();
      } catch (_) {
        // Ignore browser shutdown errors.
      }
    }
    throw e;
  }
}

async function fetchLinkedinHtml(url) {
  try {
    return await renderLinkedinPage(url);
  } catch (e) {
    log('WARN', `LinkedIn browser render failed for ${url}: ${e.message}. Falling back to static HTML.`);
    const response = await axios.get(url, {
      timeout: LINKEDIN_TIMEOUT_MS,
      headers: {
        'user-agent': process.env.LINKEDIN_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0',
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,id;q=0.8',
      },
      validateStatus: status => status >= 200 && status < 400,
    });

    return { html: response.data, finalUrl: url, renderedMeta: {} };
  }
}

function extractMetaFromHtml($, jsonLd, renderedMeta) {
  const content = cleanAscii(jsonLd.articleBody);
  const author = jsonLd.author || {};
  const postTimeStr =
    renderedMeta.postTimeStr ||
    $('[datetime]').first().attr('datetime') ||
    $('time').first().text() ||
    jsonLd.datePublished ||
    null;
  const parsedTime = parsePostTime(postTimeStr, jsonLd.datePublished);

  return {
    content,
    name: cleanAscii(author.name),
    accountUrl: author.url || null,
    postTimeStr: postTimeStr ? String(postTimeStr).replace(/ Edited| Diedit/g, '').trim() : null,
    postTimeDatetime: parsedTime.datetime,
    postTimeDatetimeMs: parsedTime.datetimeMs,
    reactionCount: parseCount(renderedMeta.reactionCount || $('span[data-test-id="social-actions__reaction-count"]').first().text()),
    commentCount: parseCount(renderedMeta.commentCount || $('[data-num-comments]').first().attr('data-num-comments')),
    avatar:
      renderedMeta.avatar ||
      $('article img').first().attr('src') ||
      $('meta[property="og:image"]').attr('content') ||
      null,
  };
}

async function fetchLinkedinPost(url) {
  const startedAt = Date.now();
  log('INFO', `LinkedIn crawl: ${url}`);

  try {
    const rendered = await fetchLinkedinHtml(url);
    const $ = cheerio.load(rendered.html);
    const jsonLd = findJsonLd($);
    const postId = extractPostId($, [url, rendered.finalUrl]);
    const meta = extractMetaFromHtml($, jsonLd, rendered.renderedMeta || {});

    if (!postId || !meta.content || !meta.name || !meta.accountUrl) {
      log('WARN', `LinkedIn data tidak lengkap: post_id=${postId || '-'} name=${meta.name || '-'} account_url=${meta.accountUrl || '-'}`);
      return { status: 'fail', items: [] };
    }

    const serverIp = await getPublicIp();
    const item = {
      post_id: postId,
      url,
      datetime_crawling_ms: startedAt,
      datetime_ms: meta.postTimeDatetimeMs,
      created_time: startedAt,
      updated_time: startedAt,
      hashtag: extractHashtags(meta.content),
      comment_count: meta.commentCount,
      reaction_count: meta.reactionCount,
      metadata: {
        crawler: {
          server_ip: serverIp,
          git_commit_id: process.env.GIT_COMMIT_ID || null,
          account: {
            user: null,
            token: null,
          },
          type: 'Guest',
          search: null,
          crawling_mode: 'inject',
          author: LINKEDIN_CRAWLER_AUTHOR,
        },
      },
      owner: {
        name: meta.name,
        url: meta.accountUrl,
        headline: null,
        avatar: meta.avatar,
      },
      post: {
        content_str: meta.content,
      },
      post_time: {
        post_time_str: meta.postTimeStr,
        post_time_datetime: meta.postTimeDatetime,
        post_time_datetime_ms: meta.postTimeDatetimeMs,
      },
    };

    log('SUCCESS', `LinkedIn crawled: ${postId}`);
    return { status: 'ok', items: [item] };
  } catch (e) {
    log('ERROR', `LinkedIn crawl failed for ${url}: ${e.message}`);
    return { status: 'fail', items: [] };
  }
}

async function dispatchLinkedinItems(items) {
  const kafkaConnected = !!getKafkaProducer();
  const results = [];

  if (!LINKEDIN_TOPIC) {
    log('WARN', 'KAFKA_TOPIC_LINKEDIN is not configured. LinkedIn Kafka dispatch skipped.');
  }

  for (const item of items) {
    const key = item.post_id;
    const result = { id: key, shortcode: key, url: item.__source_url || item.url, kafka: null, mongo: null };

    if (!key) {
      result.kafka = 'skipped';
      result.mongo = 'skipped';
      result.skipReason = 'missing_id';
      log('WARN', `LinkedIn item skipped: missing id (${result.url || '-'})`);
    } else if (kafkaConnected && LINKEDIN_TOPIC) {
      try {
        await withTimeout(produceMessage(LINKEDIN_TOPIC, key, item), `Kafka LinkedIn ${key}`);
        log('SUCCESS', `Kafka sent LinkedIn -> [${LINKEDIN_TOPIC}]: ${key}`, { shortcode: key });
        result.kafka = 'sent';
      } catch (e) {
        log('ERROR', `Kafka failed LinkedIn: ${key}: ${e.message}`, { shortcode: key });
        result.kafka = 'failed';
        result.kafkaError = e.message;
      }
    } else {
      result.kafka = kafkaConnected ? 'no_topic' : 'disabled';
    }

    if (key) {
      result.mongo = 'disabled';
      log('INFO', `Mongo skipped LinkedIn: ${key} (Kafka only)`, { shortcode: key });
    }

    results.push(result);
    broadcast('dispatch_result', result);
  }

  return results;
}

module.exports = {
  fetchLinkedinPost,
  dispatchLinkedinItems,
};
