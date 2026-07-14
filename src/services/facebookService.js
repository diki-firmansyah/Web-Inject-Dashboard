'use strict';

const crypto = require('crypto');
const { chromium } = require('playwright');
const cheerio = require('cheerio');
const axios = require('axios');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getKafkaProducer, produceMessage } = require('../config/db');

const FACEBOOK_KAFKA_TOPIC = process.env.KAFKA_TOPIC_FACEBOOK || 'facebook_post';
const FACEBOOK_CRAWLER_AUTHOR = process.env.FACEBOOK_CRAWLER_AUTHOR || 'donell';
const FACEBOOK_REQUEST_TIMEOUT_MS = Number(process.env.FACEBOOK_REQUEST_TIMEOUT_MS || 60000);
const FACEBOOK_POST_LOAD_WAIT_MS = Number(process.env.FACEBOOK_POST_LOAD_WAIT_MS || 5000);
const FACEBOOK_LOCALE = process.env.FACEBOOK_LOCALE || 'id-ID';
const FACEBOOK_USER_AGENT = process.env.FACEBOOK_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36';

const UNAVAILABLE_TEXTS = [
  'Video Ini Tidak Tersedia Lagi',
  'Konten Ini Tidak Tersedia',
  'Konten Ini Tidak Tersedia Saat Ini',
  'Halaman ini saat ini tidak tersedia',
  'This content is not available',
  'This page is not available',
];

const MONTHS = {
  januari: 0, februari: 1, maret: 2, april: 3, mei: 4, juni: 5,
  juli: 6, agustus: 7, september: 8, oktober: 9, november: 10, desember: 11,
  jan: 0, feb: 1, mar: 2, apr: 3, jun: 5, jul: 6, agu: 7, sep: 8, okt: 9, des: 11,
  january: 0, february: 1, march: 2, may: 4, june: 5, july: 6, august: 7,
  october: 9, december: 11, aug: 7, oct: 9, dec: 11,
};

function normalizeTarget(target) {
  if (target && typeof target === 'object') {
    return {
      url: String(target.url || target.link || '').trim(),
      keyword: String(target.keyword || target.keyphrase || target.search || 'inject by url').trim() || 'inject by url',
    };
  }

  const raw = String(target || '').trim();
  const [url, keyword] = raw.includes('|') ? raw.split('|', 2).map(part => part.trim()) : [raw, 'inject by url'];
  return { url, keyword: keyword || 'inject by url' };
}

function normalizeFacebookUrl(url) {
  const value = String(url || '').trim();
  if (!value) return value;
  if (/^https?:\/\//i.test(value)) return value;
  return `https://www.facebook.com/${value.replace(/^\/+/, '')}`;
}

function toNumber(text) {
  const cleaned = String(text || '').toLowerCase().replace(/\s+/g, '').replace(/[^\d,.kmbjtrbuta]/g, '');
  const match = cleaned.match(/\d+(?:[,.]\d+)?/);
  if (!match) return 0;

  const value = Number(match[0].replace(',', '.'));
  if (!Number.isFinite(value)) return 0;
  if (/(rb|ribu|k)/.test(cleaned)) return Math.round(value * 1_000);
  if (/(jt|juta|m)/.test(cleaned)) return Math.round(value * 1_000_000);
  if (/(b|miliar|billion)/.test(cleaned)) return Math.round(value * 1_000_000_000);
  return Math.round(value);
}

function parseFacebookTime(text) {
  const raw = String(text || '').replace(/\u00a0/g, ' ').trim().toLowerCase();
  if (!raw) return null;

  const now = new Date();
  if (/baru saja|just now/.test(raw)) return now;

  const relative = raw.match(/(\d+)\s*(detik|second|seconds|s|menit|minute|minutes|m|jam|hour|hours|h|hari|day|days|d|minggu|week|weeks|w)\b/);
  if (relative) {
    const amount = Number(relative[1]);
    const unit = relative[2];
    const ms = /detik|second|seconds|s/.test(unit) ? amount * 1000
      : /menit|minute|minutes|m/.test(unit) ? amount * 60_000
      : /jam|hour|hours|h/.test(unit) ? amount * 3_600_000
      : /hari|day|days|d/.test(unit) ? amount * 86_400_000
      : amount * 7 * 86_400_000;
    return new Date(now.getTime() - ms);
  }

  const yesterday = raw.match(/(?:kemarin|yesterday)\s*(?:pukul|jam|at)?\s*(\d{1,2})[:.](\d{2})?/);
  if (yesterday) {
    const date = new Date(now.getTime() - 86_400_000);
    date.setHours(Number(yesterday[1]), Number(yesterday[2] || 0), 0, 0);
    return date;
  }

  const full = raw.match(/(\d{1,2})\s+([a-z]+)\s+(\d{4})(?:\s+(?:pukul|pada|at)?\s*(\d{1,2})[:.](\d{2}))?/i);
  if (full && MONTHS[full[2]] !== undefined) {
    return new Date(Number(full[3]), MONTHS[full[2]], Number(full[1]), Number(full[4] || 0), Number(full[5] || 0), 0, 0);
  }

  const partial = raw.match(/(\d{1,2})\s+([a-z]+)(?:\s+(?:pukul|pada|at)?\s*(\d{1,2})[:.](\d{2}))?/i);
  if (partial && MONTHS[partial[2]] !== undefined) {
    return new Date(now.getFullYear(), MONTHS[partial[2]], Number(partial[1]), Number(partial[3] || 0), Number(partial[4] || 0), 0, 0);
  }

  const parsed = Date.parse(text);
  return Number.isNaN(parsed) ? null : new Date(parsed);
}

function extractJsonLd($) {
  const values = [];
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      values.push(JSON.parse($(el).text()));
    } catch (_) {}
  });
  return values.flatMap(value => Array.isArray(value) ? value : [value]);
}

function getMeta($, keys) {
  for (const key of keys) {
    const value = $(`meta[property="${key}"], meta[name="${key}"]`).attr('content');
    if (value) return value.trim();
  }
  return '';
}

function extractPostId(finalUrl, html) {
  try {
    const parsed = new URL(finalUrl);
    const qs = parsed.searchParams;
    const queryId = qs.get('story_fbid') || qs.get('fbid') || qs.get('v');
    if (queryId) return queryId;

    const pathMatch = parsed.pathname.match(/\/(?:posts|videos|reel|photos|watch|permalink|v|view)\/([^/?#]+)/i);
    if (pathMatch) return pathMatch[1];

    const numericTail = parsed.pathname.match(/\/(\d{10,})\/?$/);
    if (numericTail) return numericTail[1];
  } catch (_) {}

  const htmlMatch = String(html || '').match(/"(?:story_fbid|post_id|video_id|top_level_post_id)":"?([A-Za-z0-9_:-]+)"?/);
  return htmlMatch ? htmlMatch[1] : null;
}

function extractOwner(finalUrl, html, fullname) {
  let username = null;
  let id = null;
  let groupname = null;

  try {
    const parsed = new URL(finalUrl);
    const parts = parsed.pathname.split('/').filter(Boolean);
    if (parts[0] === 'groups' && parts[1]) {
      groupname = parts[1];
    } else if (parts[0] && !['story.php', 'permalink.php', 'photo.php', 'watch'].includes(parts[0])) {
      username = parts[0];
      if (/^\d+$/.test(username)) id = username;
    }

    id = parsed.searchParams.get('id') || id;
    username = parsed.searchParams.get('vanity') || username;
  } catch (_) {}

  const htmlId = String(html || '').match(/"(?:delegate_page_id|owning_profile_id|profile_id)":"(\d+)"/);
  if (htmlId) id = htmlId[1];

  const url = groupname
    ? `https://www.facebook.com/groups/${groupname}/`
    : (id || username) ? `https://www.facebook.com/${id || username}` : null;

  return {
    id,
    url,
    username: groupname ? null : username,
    full_name: fullname || '',
    banner: null,
    avatar: null,
    followers: { text: null, number: 0 },
    following: { text: null, number: 0 },
    groupname,
  };
}

function findTimeText($) {
  const candidates = [];
  $('a, span, abbr').each((_, el) => {
    const text = ($(el).attr('aria-label') || $(el).attr('title') || $(el).text() || '').trim();
    if (text) candidates.push(text);
  });

  return candidates.find(text => parseFacebookTime(text)) || '';
}

function extractCounts($, html) {
  const bodyText = $('body').text().replace(/\s+/g, ' ');
  const commentMatch = bodyText.match(/([\d.,]+\s*(?:rb|ribu|k|jt|juta|m)?)\s*(?:komentar|comments?)/i);
  const shareMatch = bodyText.match(/([\d.,]+\s*(?:rb|ribu|k|jt|juta|m)?)\s*(?:dibagikan|shares?)/i);
  const likeMatch = bodyText.match(/([\d.,]+\s*(?:rb|ribu|k|jt|juta|m)?)\s*(?:suka|likes?|reactions?)/i);

  const reactionJson = String(html || '').match(/"reaction_count"\s*:\s*\{"count":(\d+)/);
  const commentJson = String(html || '').match(/"comment_count"\s*:\s*\{"total_count":(\d+)/);
  const shareJson = String(html || '').match(/"share_count"\s*:\s*\{"count":(\d+)/);

  const like = reactionJson ? Number(reactionJson[1]) : toNumber(likeMatch?.[1]);
  const comment = commentJson ? Number(commentJson[1]) : toNumber(commentMatch?.[1]);
  const share = shareJson ? Number(shareJson[1]) : toNumber(shareMatch?.[1]);

  return { like, likeText: likeMatch?.[1] || String(like || 0), comment, share };
}

function extractMedia($, html, lastVideoUrl) {
  const image = getMeta($, ['og:image', 'twitter:image'])
    || $('img[src*="scontent"]').first().attr('src')
    || null;

  const video = lastVideoUrl
    || getMeta($, ['og:video', 'og:video:url', 'twitter:player:stream'])
    || (String(html || '').match(/"playable_url(?:_quality_hd)?"\s*:\s*"([^"]+)"/)?.[1] || '').replace(/\\\//g, '/').replace(/\\u0025/g, '%')
    || null;

  return { image, video };
}

function extractTitleAndCaption($, jsonLd) {
  const metaTitle = getMeta($, ['og:title', 'twitter:title']);
  const metaDescription = getMeta($, ['og:description', 'description', 'twitter:description']);
  const ld = jsonLd.find(item => item && typeof item === 'object' && (item.articleBody || item.text || item.description || item.name)) || {};

  const caption = (ld.articleBody || ld.text || metaDescription || '').trim();
  let fullname = '';
  if (ld.author) {
    fullname = typeof ld.author === 'string' ? ld.author : (ld.author.name || '');
  }
  if (!fullname && metaTitle.includes('|')) fullname = metaTitle.split('|').pop().trim();
  if (!fullname && metaTitle.includes(' - ')) fullname = metaTitle.split(' - ').pop().trim();

  return {
    fullname,
    caption,
    title: metaTitle,
  };
}

async function getServerIp() {
  try {
    const ip = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
    return ip.data?.ip || null;
  } catch (_) {
    return null;
  }
}

function buildFacebookPayload({ sourceUrl, finalUrl, keyword, html, lastVideoUrl, crawlingStart, serverIp }) {
  const $ = cheerio.load(html || '');
  const jsonLd = extractJsonLd($);
  const { fullname, caption, title } = extractTitleAndCaption($, jsonLd);
  const timeText = findTimeText($);
  const postDate = parseFacebookTime(timeText);
  const now = new Date();
  const crawlingFinish = Date.now();
  const counts = extractCounts($, html);
  const media = extractMedia($, html, lastVideoUrl);
  const id = extractPostId(finalUrl, html);
  const owner = extractOwner(finalUrl, html, fullname);
  const hashHex = crypto.createHash('md5').update(`${owner.full_name || ''}${caption || ''}${id || finalUrl}`).digest('hex');

  return {
    index: hashHex,
    actions: [],
    actions_json: null,
    actions_link: finalUrl,
    actions_name: null,
    message: caption,
    id,
    image: media.image,
    video_url: media.video,
    images: media.image ? [media.image] : [],
    link: finalUrl,
    description: caption,
    like_count: { text: counts.likeText, number: counts.like },
    comment_count: counts.comment,
    share_count: counts.share,
    reaction_count: counts.like,
    datetime_str: postDate ? postDate.toISOString() : null,
    datetime_ms: postDate ? postDate.getTime() : null,
    datetime_crawling_ms: now.getTime(),
    datetime_crawling_str: now.toISOString(),
    owner: {
      id: owner.id,
      url: owner.url,
      username: owner.username,
      full_name: owner.full_name,
      banner: owner.banner,
      avatar: owner.avatar,
      followers: owner.followers,
      following: owner.following,
    },
    meta: {
      url_group: owner.groupname ? owner.url : null,
      group_id: null,
      picture_url: null,
      full_name: owner.full_name,
      link_profile: owner.url,
      username: owner.username,
      small_pict_url: null,
      type: 'keyword',
      source_url: sourceUrl,
      page_title: title,
    },
    metadata: {
      crawler: {
        server: serverIp || null,
        account: { user: '-' },
        git_commit: process.env.GIT_COMMIT_ID || null,
        git_version: '2.0.1',
        source: 'keyword',
        search: keyword,
        client_id: null,
        type: 'Guest_2025',
        author: FACEBOOK_CRAWLER_AUTHOR,
      },
      crawling_time: {
        start: crawlingStart,
        finish: crawlingFinish,
        duration: crawlingFinish - crawlingStart,
      },
    },
    need_loadmore: true,
    source: 'google_search_facebook',
    created_at: now.toISOString(),
    updated_at: now.toISOString(),
  };
}

async function fetchFacebookPost(target) {
  const { url: rawUrl, keyword } = normalizeTarget(target);
  const sourceUrl = normalizeFacebookUrl(rawUrl);
  const crawlingStart = Date.now();

  if (!sourceUrl) {
    return { status: 'error', error: 'URL Facebook kosong', items: [] };
  }

  log('INFO', `Facebook URL inject: ${sourceUrl}`, { url: sourceUrl });

  let browser;
  let lastVideoUrl = null;

  try {
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      locale: FACEBOOK_LOCALE,
      userAgent: FACEBOOK_USER_AGENT,
      viewport: { width: 1365, height: 900 },
    });
    const page = await context.newPage();

    page.on('response', response => {
      const responseUrl = response.url();
      if (!lastVideoUrl && responseUrl.includes('.mp4') && /video|fbcdn|scontent/i.test(responseUrl)) {
        lastVideoUrl = responseUrl;
      }
    });

    await page.goto(sourceUrl, { waitUntil: 'domcontentloaded', timeout: FACEBOOK_REQUEST_TIMEOUT_MS });
    try {
      await page.waitForLoadState('networkidle', { timeout: Math.min(FACEBOOK_REQUEST_TIMEOUT_MS, 30000) });
    } catch (_) {}
    await page.waitForTimeout(FACEBOOK_POST_LOAD_WAIT_MS);

    for (const text of UNAVAILABLE_TEXTS) {
      try {
        if (await page.locator(`text=${text}`).first().isVisible({ timeout: 500 })) {
          throw new Error(`Content not available: ${text}`);
        }
      } catch (e) {
        if (/Content not available/.test(e.message)) throw e;
      }
    }

    const finalUrl = page.url();
    const html = await page.content();
    await browser.close();
    browser = null;

    const serverIp = await getServerIp();
    const item = buildFacebookPayload({ sourceUrl, finalUrl, keyword, html, lastVideoUrl, crawlingStart, serverIp });

    if (!item.id && !item.description && !item.image && !item.video_url) {
      throw new Error('Data Facebook tidak ditemukan di halaman');
    }

    log('SUCCESS', `Facebook fetched: ${item.id || item.index}`, { url: sourceUrl });
    return { status: 'ok', items: [item] };
  } catch (e) {
    if (browser) await browser.close().catch(() => {});
    log('ERROR', `Facebook fetch failed ${sourceUrl}: ${e.message}`, { url: sourceUrl });
    return { status: 'error', error: e.message, items: [] };
  }
}

async function dispatchFacebookItems(items) {
  const results = [];
  const kafkaConnected = !!getKafkaProducer();

  for (const item of items) {
    const id = item.id;
    const r = { id, shortcode: id, url: item.__source_url || item.link || item.actions_link, kafka: null, mongo: 'disabled' };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `Facebook item skipped: missing id (${r.url || '-'})`);
    } else {
      if (kafkaConnected && FACEBOOK_KAFKA_TOPIC) {
        try {
          await produceMessage(FACEBOOK_KAFKA_TOPIC, id, item);
          log('SUCCESS', `Kafka sent Facebook -> [${FACEBOOK_KAFKA_TOPIC}]: ${id}`, { shortcode: id });
          r.kafka = 'sent';
        } catch (e) {
          log('ERROR', `Kafka failed Facebook: ${id}: ${e.message}`, { shortcode: id });
          r.kafka = 'failed';
          r.kafkaError = e.message;
        }
      } else {
        r.kafka = kafkaConnected ? 'no_topic' : 'disabled';
      }

      log('INFO', `Mongo skipped Facebook: ${id} (Kafka only)`, { shortcode: id });
    }

    results.push(r);
    broadcast('dispatch_result', r);
  }

  return results;
}

module.exports = {
  fetchFacebookPost,
  dispatchFacebookItems,
};
