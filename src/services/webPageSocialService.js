'use strict';

const { chromium } = require('playwright');
const axios = require('axios');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getMongoCollection, getKafkaProducer, produceMessage } = require('../config/db');

const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36';

function asNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const cleaned = String(value).replace(/,/g, '').trim();
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function formatWIBDateTime(ms) {
  return new Date(ms + 7 * 3600_000).toISOString().replace('T', ' ').slice(0, 19);
}

function formatWIBIso(ms) {
  return new Date(ms + 7 * 3600_000).toISOString().replace('.000Z', '+07:00');
}

function formatTwitterDate(ms) {
  if (!Number.isFinite(ms)) return null;
  return new Date(ms).toUTCString().replace(',', '');
}

function formatUtcCreatedTime(ms) {
  return new Date(ms).toISOString().replace('Z', '000');
}

function parseSimpleCount(text) {
  if (!text) return null;
  const value = String(text).toLowerCase().replace(/,/g, '').trim();
  const m = value.match(/([\d.]+)\s*(k|m|b|rb|jt|ribu|juta)?/i);
  if (!m) return null;

  const base = Number(m[1]);
  if (!Number.isFinite(base)) return null;

  const suffix = m[2];
  if (!suffix) return Math.round(base);
  if (suffix === 'k' || suffix === 'rb' || suffix === 'ribu') return Math.round(base * 1_000);
  if (suffix === 'm' || suffix === 'jt' || suffix === 'juta') return Math.round(base * 1_000_000);
  if (suffix === 'b') return Math.round(base * 1_000_000_000);
  return Math.round(base);
}

function normalizeUrl(rawUrl, defaultProtocol = 'https://') {
  const trimmed = String(rawUrl || '').trim();
  if (!trimmed) return '';
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  return defaultProtocol + trimmed;
}

function extractMeta(html, property) {
  const patterns = [
    new RegExp(`<meta[^>]+property=["']${property}["'][^>]+content=["']([^"']*)["']`, 'i'),
    new RegExp(`<meta[^>]+content=["']([^"']*)["'][^>]+property=["']${property}["']`, 'i'),
    new RegExp(`<meta[^>]+name=["']${property}["'][^>]+content=["']([^"']*)["']`, 'i'),
    new RegExp(`<meta[^>]+content=["']([^"']*)["'][^>]+name=["']${property}["']`, 'i'),
  ];

  for (const pattern of patterns) {
    const m = html.match(pattern);
    if (m) return decodeHtml(m[1]);
  }
  return null;
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function parseTitle(html) {
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m ? decodeHtml(m[1].trim()) : null;
}

function safeJsonParse(value) {
  try {
    return JSON.parse(value);
  } catch (_) {
    return null;
  }
}

function findYouTubeInitialPlayerResponse(html) {
  const marker = 'ytInitialPlayerResponse';
  const idx = html.indexOf(marker);
  if (idx === -1) return null;

  const start = html.indexOf('{', idx);
  if (start === -1) return null;

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = start; i < html.length; i++) {
    const ch = html[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') inString = true;
    if (ch === '{') depth++;
    if (ch === '}') depth--;
    if (depth === 0) return safeJsonParse(html.slice(start, i + 1));
  }

  return null;
}

function findYouTubeInitialData(html) {
  const marker = 'ytInitialData';
  const idx = html.indexOf(marker);
  if (idx === -1) return null;

  const start = html.indexOf('{', idx);
  if (start === -1) return null;

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let i = start; i < html.length; i++) {
    const ch = html[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }

    if (ch === '"') inString = true;
    if (ch === '{') depth++;
    if (ch === '}') depth--;
    if (depth === 0) return safeJsonParse(html.slice(start, i + 1));
  }

  return null;
}

function walkJson(value, visitor) {
  if (!value || typeof value !== 'object') return;
  visitor(value);
  if (Array.isArray(value)) {
    for (const item of value) walkJson(item, visitor);
    return;
  }
  for (const item of Object.values(value)) walkJson(item, visitor);
}

function findFirstTextByKey(root, keyName) {
  let found = null;
  walkJson(root, node => {
    if (found || !node || typeof node !== 'object') return;
    const target = node[keyName];
    if (!target) return;
    found = target.simpleText || target.runs?.map(run => run.text).join('') || null;
  });
  return found;
}

function extractYouTubeInteractionCounts(initialData) {
  let like = null;
  let commentCount = null;

  walkJson(initialData, node => {
    if (!node || typeof node !== 'object') return;

    const segmentedLike = node.segmentedLikeDislikeButtonViewModel?.likeButtonViewModel?.likeButtonViewModel;
    const likeContent = segmentedLike?.toggleButtonViewModel?.toggleButtonViewModel?.defaultButtonViewModel?.buttonViewModel?.title;
    if (like === null && likeContent) like = parseSimpleCount(likeContent);

    const accessibilityLabel = node.accessibilityData?.label || node.accessibility?.accessibilityData?.label;
    if (like === null && /like/i.test(String(accessibilityLabel || ''))) {
      like = parseSimpleCount(accessibilityLabel);
    }

    if (commentCount === null && node.commentsHeaderRenderer?.countText) {
      const countText = node.commentsHeaderRenderer.countText;
      const text = countText.simpleText || countText.runs?.map(run => run.text).join('');
      commentCount = parseSimpleCount(text);
    }
  });

  return { like, commentCount };
}

function extractYouTubeChannelAvatar(initialData) {
  let avatar = null;
  walkJson(initialData, node => {
    if (avatar || !node || typeof node !== 'object') return;
    const thumbnails = node.videoOwnerRenderer?.thumbnail?.thumbnails;
    if (Array.isArray(thumbnails) && thumbnails.length) {
      avatar = thumbnails.at(-1)?.url || null;
    }
  });
  return avatar;
}

function baseItem({ platform, url, externalId, title, description, author, imageUrl, videoUrl, publishedAt, stats, raw, nowMs }) {
  const finishMs = Date.now();
  const datetimeMs = publishedAt ? Date.parse(publishedAt) : null;

  return {
    post_id: externalId || url,
    platform,
    post_url: url,
    username: author?.username || null,
    author_id: author?.id || null,
    author: {
      id: author?.id || null,
      username: author?.username || null,
      name: author?.name || null,
      profile_url: author?.profileUrl || null,
    },
    caption: description || title || null,
    title: title || null,
    description: description || null,
    like_count: stats?.likeCount ?? null,
    comment_count: stats?.commentCount ?? null,
    share_count: stats?.shareCount ?? null,
    view_count: stats?.viewCount ?? null,
    datetime_ms: Number.isFinite(datetimeMs) ? datetimeMs : null,
    datetime_str: Number.isFinite(datetimeMs) ? new Date(datetimeMs).toISOString() : null,
    media: {
      image: imageUrl ? [{ url: imageUrl }] : [],
      video: videoUrl ? [{ url: videoUrl }] : [],
    },
    raw,
    scrap_type: `${platform} inject by link`,
    created_time: new Date(finishMs).toISOString(),
    updated_time: new Date(finishMs).toISOString(),
    metadata: {
      crawler: {
        server_ip: 'unknown',
        version: '1',
        account: { user: 'guest', token: null },
        type: 'non-login',
        search: externalId || url,
        search_type: `${platform}-inject-by-link`,
        git_commit_id: null,
        author: 'webinject-dashboard',
      },
      crawling_time: {
        start: nowMs,
        finish: finishMs,
        duration: finishMs - nowMs,
      },
      status: 'crawled',
    },
  };
}

function parseYouTube(_platform, html, url, nowMs, helpers) {
  const player = findYouTubeInitialPlayerResponse(html);
  const initialData = findYouTubeInitialData(html);
  const details = player?.videoDetails || {};
  const micro = player?.microformat?.playerMicroformatRenderer || {};
  const externalId = helpers.extractId(url) || details.videoId || extractMeta(html, 'og:video:url');
  const ownerProfileUrl = micro.ownerProfileUrl || null;
  const finishMs = Date.now();
  const title = details.title || extractMeta(html, 'og:title') || parseTitle(html);
  const description = details.shortDescription || extractMeta(html, 'og:description') || null;
  const thumbnail = micro.thumbnail?.thumbnails?.at(-1) || details.thumbnail?.thumbnails?.at(-1);
  const publishedAt = micro.publishDate || micro.uploadDate || null;
  const datetimeMs = publishedAt ? Date.parse(publishedAt) : null;
  const channelUrl = ownerProfileUrl || (micro.ownerProfileUrl ? `https://www.youtube.com${micro.ownerProfileUrl}` : null);
  const channelId = ownerProfileUrl ? ownerProfileUrl.split('/').filter(Boolean).pop() : (details.channelId || micro.externalChannelId || null);
  const counts = extractYouTubeInteractionCounts(initialData);
  const subscriberText = findFirstTextByKey(initialData, 'subscriberCountText');
  const channelAvatar = extractYouTubeChannelAvatar(initialData);

  return {
    vid: externalId,
    channel_avatar: channelAvatar,
    channel_bio: null,
    channel_desc: null,
    channel_detail: {
      for_business_inquiries: null,
      location: micro.availableCountries?.[0] || null,
    },
    channel_join_date: null,
    channel_join_date_ms: null,
    channel_join_date_str: null,
    channel_share_link: {},
    channel_url: channelUrl,
    channel_view_count: 0,
    crawler_type: 'youtube_by_url',
    datetime_crawling_ms: nowMs,
    datetime_crawling_str: formatWIBDateTime(nowMs),
    datetime_ms: Number.isFinite(datetimeMs) ? datetimeMs : null,
    datetime_str: Number.isFinite(datetimeMs) ? formatWIBIso(datetimeMs) : null,
    etag: '',
    id: {
      kind: 'youtube#video',
      videoId: externalId,
    },
    kind: 'youtube#channel',
    search_query: details.author || micro.ownerChannelName || null,
    snippet: {
      publishedAt: publishedAt || null,
      channelId,
      title,
      like: counts.like,
      dislike: null,
      comment_count: counts.commentCount,
      description,
      description_html: null,
      thumbnails: thumbnail ? [{
        url: thumbnail.url,
        width: thumbnail.width || null,
        height: thumbnail.height || null,
      }] : [],
      channelTitle: details.author || micro.ownerChannelName || null,
      liveBroadcastContent: micro.liveBroadcastDetails?.isLiveNow ?? null,
    },
    subscriber_count: parseSimpleCount(subscriberText),
    viewer_count: asNumber(details.viewCount),
    created_time: formatWIBDateTime(nowMs),
    updated_time: formatWIBDateTime(nowMs),
    metadata: {
      crawler: {
        server_ip: helpers.serverIp || 'unknown',
        git_commit_id: null,
        account: {
          user: null,
          token: null,
        },
        type: 'guest_2025',
        search: details.author || micro.ownerChannelName || null,
        search_type: 'url',
        author: 'webinject-dashboard',
      },
      crawling_time: {
        start: nowMs,
        finish: finishMs,
        duration: finishMs - nowMs,
      },
    },
  };
}

function parseOpenGraph(platform, html, url, nowMs, helpers) {
  const title = extractMeta(html, 'og:title') || extractMeta(html, 'twitter:title') || parseTitle(html);
  const description = extractMeta(html, 'og:description') || extractMeta(html, 'twitter:description');
  const imageUrl = extractMeta(html, 'og:image') || extractMeta(html, 'twitter:image');
  const videoUrl = extractMeta(html, 'og:video') || extractMeta(html, 'og:video:url');
  const canonicalUrl = extractMeta(html, 'og:url') || url;
  const authorName = extractMeta(html, 'article:author') || null;

  return baseItem({
    platform,
    url: canonicalUrl,
    externalId: helpers.extractId(canonicalUrl) || helpers.extractId(url) || canonicalUrl,
    title,
    description,
    author: helpers.extractAuthor(canonicalUrl, title, authorName),
    imageUrl,
    videoUrl,
    publishedAt: extractMeta(html, 'article:published_time') || null,
    stats: {},
    raw: {
      title,
      description,
      imageUrl,
      videoUrl,
      canonicalUrl,
    },
    nowMs,
  });
}

function parseTwitter(_platform, html, url, nowMs, helpers) {
  const canonicalUrl = extractMeta(html, 'og:url') || url;
  const tweetId = helpers.extractId(canonicalUrl) || helpers.extractId(url);
  const author = helpers.extractAuthor(canonicalUrl, null, null);
  const title = extractMeta(html, 'og:title') || extractMeta(html, 'twitter:title') || parseTitle(html);
  const description = extractMeta(html, 'og:description') || extractMeta(html, 'twitter:description') || null;
  const imageUrl = extractMeta(html, 'og:image') || extractMeta(html, 'twitter:image');
  const publishedAt = extractMeta(html, 'article:published_time') || null;
  const datetimeMs = publishedAt ? Date.parse(publishedAt) : null;
  const finishMs = Date.now();
  const cookieToken = helpers.cookieHeader || null;
  const csrfToken = helpers.cookies?.ct0 || null;
  const verified = /verified/i.test(String(title || '')) ? true : false;

  const idNumber = tweetId && /^\d+$/.test(tweetId) ? Number(tweetId) : null;
  const tweetText = description || title || null;
  const screenName = author?.username || null;

  return {
    created_at: Number.isFinite(datetimeMs) ? formatTwitterDate(datetimeMs) : null,
    id: idNumber,
    id_str: tweetId || null,
    text: tweetText,
    source: null,
    truncated: false,
    in_reply_to_status_id: null,
    in_reply_to_status_id_str: null,
    in_reply_to_user_id: null,
    in_reply_to_user_id_str: null,
    in_reply_to_screen_name: null,
    user: {
      id: null,
      id_str: null,
      name: author?.name || screenName,
      screen_name: screenName,
      location: null,
      url: screenName ? `https://x.com/${screenName}` : null,
      description: null,
      translator_type: 'none',
      protected: false,
      verified,
      followers_count: null,
      friends_count: null,
      listed_count: null,
      favourites_count: null,
      statuses_count: null,
      created_at: null,
      utc_offset: null,
      time_zone: null,
      geo_enabled: null,
      lang: null,
      contributors_enabled: null,
      is_translator: false,
      profile_background_color: null,
      profile_background_image_url: null,
      profile_background_image_url_https: null,
      profile_background_tile: null,
      profile_link_color: null,
      profile_sidebar_border_color: null,
      profile_sidebar_fill_color: null,
      profile_text_color: null,
      profile_use_background_image: null,
      profile_image_url: imageUrl || null,
      profile_image_url_https: imageUrl || null,
      profile_banner_url: null,
      default_profile: false,
      default_profile_image: false,
      following: null,
      follow_request_sent: null,
      notifications: null,
    },
    geo: null,
    coordinates: null,
    place: null,
    contributors: null,
    quoted_status_id: null,
    quoted_status_id_str: null,
    quoted_status: null,
    quoted_status_permalink: {
      url: null,
      expanded: null,
      display: null,
    },
    is_quote_status: false,
    quote_count: null,
    reply_count: null,
    retweet_count: null,
    favorite_count: null,
    bookmark_count: null,
    views: null,
    entities: {},
    favorited: false,
    retweeted: false,
    filter_level: null,
    lang: null,
    timestamp_ms: Number.isFinite(datetimeMs) ? Math.floor(datetimeMs / 1000) : null,
    datetime_str: Number.isFinite(datetimeMs) ? formatTwitterDate(datetimeMs) : null,
    datetime_ms: Number.isFinite(datetimeMs) ? datetimeMs : null,
    keyword_search: null,
    metadata: {
      crawler: {
        server_ip: helpers.serverIp || 'unknown',
        git_commit_id: null,
        account: {
          user: null,
          token: cookieToken,
          x_csrf_token: csrfToken,
          verified: false,
        },
        type: cookieToken ? 'login' : 'guest',
        search: tweetId || canonicalUrl,
        lang: null,
        page: null,
        search_type: 'inject-post-url',
        client_id: null,
        author: 'webinject-dashboard',
      },
      crawling_time: {
        start: nowMs,
        finish: finishMs,
        duration: finishMs - nowMs,
      },
    },
    created_time: formatUtcCreatedTime(nowMs),
    updated_time: null,
  };
}

function createPageCrawlerService(options) {
  const {
    platform,
    label,
    collectionEnv,
    collectionDefault,
    topicEnv,
    extractId,
    extractAuthor,
    parse = parseOpenGraph,
  } = options;

  const topicName = process.env[topicEnv];

  async function fetchPost(rawUrl) {
    const nowMs = Date.now();
    const url = normalizeUrl(rawUrl);
    log('INFO', `Fetching ${label}: ${url}`, { url });

    let browser;
    try {
      browser = await chromium.launch({ headless: true });
      const context = await browser.newContext({
        userAgent: process.env.SOCIAL_USER_AGENT || DEFAULT_USER_AGENT,
        locale: 'en-US',
      });
      const page = await context.newPage();

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 150000 });
      try {
        await page.waitForLoadState('networkidle', { timeout: 30000 });
      } catch (_) {
        log('WARN', `Timeout waiting for networkidle on ${label} ${url}, continuing.`);
      }
      await page.waitForTimeout(2500);

      const html = await page.content();
      const cookies = await context.cookies();
      const cookieHeader = cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; ');
      const cookieMap = Object.fromEntries(cookies.map(cookie => [cookie.name, cookie.value]));
      await browser.close();

      let serverIp = 'unknown';
      try {
        const ip = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
        serverIp = ip.data?.ip || 'unknown';
      } catch (_) {}

      const item = parse(platform, html, url, nowMs, { extractId, extractAuthor, serverIp, cookies: cookieMap, cookieHeader });
      const itemId = item.post_id || item.vid || item.id?.videoId || item.id_str || item.id || item.post_url;
      if (!itemId) throw new Error('Post id could not be resolved');

      log('SUCCESS', `Fetched ${label} item ${itemId}`, { url });
      return { status: 'ok', items: [item] };
    } catch (err) {
      if (browser) await browser.close();
      log('ERROR', `Failed to fetch ${label} post ${url}: ${err.message}`, { url });
      return { status: 'error', error: err.message, items: [] };
    }
  }

  async function dispatchItems(items) {
    const results = [];
    const mongoCollection = getMongoCollection(process.env[collectionEnv] || collectionDefault);
    const kafkaConnected = !!getKafkaProducer();

    if (!topicName) {
      log('WARN', `${topicEnv} is not configured. ${label} Kafka dispatch skipped.`);
    }

    for (const item of items) {
      const id = item.post_id || item.vid || item.id?.videoId || item.id_str || item.id;
      const r = { id, shortcode: id, kafka: null, mongo: null };

      if (!id) {
        r.kafka = 'skipped';
        r.mongo = 'skipped';
        r.skipReason = 'missing_id';
        log('WARN', `${label} item skipped: missing id (${item.__source_url || item.post_url || item.url || '-'})`);
      } else {
        if (kafkaConnected && topicName) {
          try {
            await produceMessage(topicName, id, item);
            log('SUCCESS', `Kafka sent ${label} -> [${topicName}]: ${id}`, { shortcode: id });
            r.kafka = 'sent';
          } catch (e) {
            log('ERROR', `Kafka failed ${label}: ${id}: ${e.message}`, { shortcode: id });
            r.kafka = 'failed';
            r.kafkaError = e.message;
          }
        } else {
          r.kafka = kafkaConnected ? 'no_topic' : 'disabled';
        }

        if (mongoCollection) {
          try {
            const filter = item.id_str ? { id_str: item.id_str } : { post_id: id };
            await mongoCollection.updateOne(filter, { $set: item }, { upsert: true });
            log('SUCCESS', `MongoDB upserted ${label}: ${id}`, { shortcode: id });
            r.mongo = 'saved';
          } catch (e) {
            log('ERROR', `Mongo failed ${label}: ${id}: ${e.message}`, { shortcode: id });
            r.mongo = 'failed';
            r.mongoError = e.message;
          }
        } else {
          r.mongo = 'disabled';
        }
      }

      results.push(r);
      broadcast('dispatch_result', r);
    }

    return results;
  }

  return { fetchPost, dispatchItems };
}

module.exports = {
  createPageCrawlerService,
  parseOpenGraph,
  parseTwitter,
  parseYouTube,
};
