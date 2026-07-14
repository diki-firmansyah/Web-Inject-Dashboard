'use strict';

const axios = require('axios');
const cheerio = require('cheerio');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getKafkaProducer, produceMessage } = require('../config/db');

const THREADS_KAFKA_TOPIC = process.env.KAFKA_TOPIC_THREADS;
const THREADS_KAFKA_FAILED_TOPIC = process.env.KAFKA_TOPIC_THREADS_FAILED || process.env.KAFKA_TOPIC_FAILED_THREADS;
const THREADS_CRAWLER_AUTHOR = process.env.THREADS_CRAWLER_AUTHOR || 'donel';
const THREADS_XMT = process.env.THREADS_XMT || 'AQF03e6fw8e6HjAMixUpWZVJtOCN2Ir2XZO8d0Ta7ay8OA';

function parseCookieHeader(cookieHeader) {
  const cookies = {};
  for (const part of String(cookieHeader || '').split(';')) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const index = trimmed.indexOf('=');
    if (index === -1) continue;
    cookies[trimmed.slice(0, index)] = trimmed.slice(index + 1);
  }
  return cookies;
}

function get(obj, path, fallback = null) {
  let value = obj;
  for (const key of path) {
    if (value === null || value === undefined) return fallback;
    value = value[key];
  }
  return value === undefined ? fallback : value;
}

function asObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
}

function formatDate(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

function formatDateTimeWIB(ms) {
  return new Date(ms + 7 * 3600_000).toISOString().replace('T', ' ').slice(0, 19);
}

function formatCreatedTime() {
  return new Date().toISOString();
}

function extractThreadCode(url) {
  const m = String(url).match(/threads\.(?:net|com)\/@?[^/]+\/post\/([A-Za-z0-9_-]+)/i);
  if (m) return m[1];
  const fallback = String(url).split('/').filter(Boolean).pop();
  return fallback || null;
}

function buildThreadsHeaders() {
  return {
    accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': process.env.THREADS_ACCEPT_LANGUAGE || 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    dpr: '2',
    priority: 'u=0, i',
    'sec-ch-prefers-color-scheme': 'light',
    'sec-ch-ua': process.env.THREADS_SEC_CH_UA || '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    'sec-ch-ua-full-version-list': process.env.THREADS_SEC_CH_UA_FULL_VERSION_LIST || '"Not:A-Brand";v="99.0.0.0", "Google Chrome";v="145.0.7632.162", "Chromium";v="145.0.7632.162"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': process.env.THREADS_SEC_CH_UA_PLATFORM || '"macOS"',
    'sec-ch-ua-platform-version': process.env.THREADS_SEC_CH_UA_PLATFORM_VERSION || '"26.3.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': process.env.THREADS_USER_AGENT || 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'viewport-width': process.env.THREADS_VIEWPORT_WIDTH || '1440',
  };
}

function findThreadsMedia(html) {
  const $ = cheerio.load(html);
  let rawData = null;

  $('script[type="application/json"]').each((_index, script) => {
    if (rawData) return;
    const content = $(script).text();
    if (!content.includes('"logging_info_token"')) return;
    try {
      rawData = JSON.parse(content);
    } catch (_) {}
  });

  if (!rawData) return null;
  return get(rawData, ['require', 0, 3, 0, '__bbox', 'require', 0, 3, 1, '__bbox', 'result', 'data', 'media'], null);
}

async function getPublicIp() {
  try {
    const response = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
    return response.data?.ip || 'Unable to determine IP';
  } catch (_) {
    return 'Unable to determine IP';
  }
}

function mapThreadsMedia(data, linkTarget, timing, serverIp) {
  const textPostAppInfo = asObject(data.text_post_app_info);
  const pinnedPostInfo = asObject(textPostAppInfo.pinned_post_info);
  const shareInfo = asObject(textPostAppInfo.share_info);
  const linkPreviewAttachment = asObject(textPostAppInfo.link_preview_attachment);
  const linkedInlineMedia = asObject(textPostAppInfo.linked_inline_media);
  const privateReplyPartner = asObject(textPostAppInfo.private_reply_partner);
  const fediverseInfo = asObject(textPostAppInfo.fediverse_info);
  const replyToAuthor = asObject(textPostAppInfo.reply_to_author);
  const caption = asObject(data.caption);
  const genAiDetectionMethod = asObject(data.gen_ai_detection_method);
  const user = asObject(data.user);
  const takenAt = data.taken_at || null;
  const datetimeMs = takenAt ? Number(`${takenAt}000`) : null;
  const datePost = takenAt ? formatDateTimeWIB(takenAt * 1000) : null;
  const code = data.code || extractThreadCode(linkTarget);
  const createdTime = formatCreatedTime();

  return {
    post_url: code ? `https://www.threads.com/post/${code}` : linkTarget,
    date_post: datePost,
    datetime_ms: datetimeMs,
    pk: data.pk ?? null,
    user_id: user.id ?? null,
    user_username: user.username ?? null,
    user_fullname: user.full_name ?? null,
    user_image: user.profile_pic_url ?? null,
    text_post_app_info: {
      is_post_unavailable: textPostAppInfo.is_post_unavailable ?? null,
      pinned_post_info: {
        is_pinned_to_profile: pinnedPostInfo.is_pinned_to_profile ?? null,
        is_pinned_to_parent_post: pinnedPostInfo.is_pinned_to_parent_post ?? null,
      },
      share_info: {
        reposted_post: shareInfo.reposted_post ?? null,
        is_reposted_by_viewer: shareInfo.is_reposted_by_viewer ?? null,
        can_quote_post: shareInfo.can_quote_post ?? null,
        quoted_post: shareInfo.quoted_post ?? null,
      },
      can_private_reply: textPostAppInfo.can_private_reply ?? null,
      can_reply: textPostAppInfo.can_reply ?? null,
      reshare_count: textPostAppInfo.reshare_count ?? null,
      is_markup: textPostAppInfo.is_markup ?? null,
      direct_reply_count: textPostAppInfo.direct_reply_count ?? null,
      repost_count: textPostAppInfo.repost_count ?? null,
      quote_count: textPostAppInfo.quote_count ?? null,
      reply_control: textPostAppInfo.reply_control ?? null,
      is_reply: textPostAppInfo.is_reply ?? null,
      link_preview_attachment: {
        display_url: linkPreviewAttachment.display_url ?? null,
        image_url: linkPreviewAttachment.image_url ?? null,
        title: linkPreviewAttachment.title ?? null,
        url: linkPreviewAttachment.url ?? null,
      },
      linked_inline_media: {
        media_type: linkedInlineMedia.media_type ?? null,
        is_paid_partnership: linkedInlineMedia.is_paid_partnership ?? null,
        carousel_media: linkedInlineMedia.carousel_media ?? null,
        code: linkedInlineMedia.code ?? null,
        image_versions2: linkedInlineMedia.image_versions2 ?? null,
        original_height: linkedInlineMedia.original_height ?? null,
        original_width: linkedInlineMedia.original_width ?? null,
        video_versions: linkedInlineMedia.video_versions ?? null,
        audio: linkedInlineMedia.audio ?? null,
        caption: linkedInlineMedia.caption ?? null,
        caption_is_edited: linkedInlineMedia.caption_is_edited ?? null,
        pk: linkedInlineMedia.pk ?? null,
        transcription_data: linkedInlineMedia.transcription_data ?? null,
        user: linkedInlineMedia.user ?? null,
        accessibility_caption: linkedInlineMedia.accessibility_caption ?? null,
        text_post_app_info: linkedInlineMedia.text_post_app_info ?? null,
        has_audio: linkedInlineMedia.has_audio ?? null,
        id: linkedInlineMedia.id ?? null,
      },
      search_trend_info_from_link_preview: textPostAppInfo.search_trend_info_from_link_preview ?? null,
      private_reply_partner: {
        username: privateReplyPartner.username ?? null,
        id: privateReplyPartner.id ?? null,
      },
      fediverse_info__is_federated: fediverseInfo.is_federated ?? null,
      post_unavailable_reason: textPostAppInfo.post_unavailable_reason ?? null,
      reply_to_author: {
        username: replyToAuthor.username ?? null,
        id: replyToAuthor.id ?? null,
      },
      related_trends_info: textPostAppInfo.related_trends_info ?? null,
      hush_info: textPostAppInfo.hush_info ?? null,
    },
    id: data.id ?? null,
    is_paid_partnership: data.is_paid_partnership ?? null,
    code,
    carousel_media: data.carousel_media ?? [],
    image_versions2: data.image_versions2 ?? null,
    video_versions: data.video_versions ?? null,
    caption: {
      pk: caption.pk ?? null,
      text: caption.text ?? null,
    },
    media_overlay_info: data.media_overlay_info ?? null,
    like_count: data.like_count ?? null,
    logging_info_token: data.logging_info_token ?? null,
    audio: data.audio ?? null,
    caption_is_edited: data.caption_is_edited ?? null,
    transcription_data: data.transcription_data ?? null,
    accessibility_caption: data.accessibility_caption ?? null,
    has_audio: data.has_audio ?? null,
    media_type: data.media_type ?? null,
    has_liked: data.has_liked ?? null,
    is_fb_only: data.is_fb_only ?? null,
    is_internal_only: data.is_internal_only ?? null,
    caption_add_on: data.caption_add_on ?? null,
    taken_at: takenAt,
    giphy_media_info: data.giphy_media_info ?? null,
    meta_place: data.metaPlace ?? null,
    organic_tracking_token: data.organic_tracking_token ?? null,
    gen_ai_detection_method: {
      detection_method: genAiDetectionMethod.detection_method ?? null,
    },
    like_and_view_counts_disabled: data.like_and_view_counts_disabled ?? null,
    datetime_crawling: {
      datetime_ms: timing.datetimeCrawlingMs,
      datetime: formatDate(timing.datetimeCrawlingMs),
    },
    created_time: createdTime,
    updated_time: createdTime,
    metadata: {
      crawler: {
        version: '1.0.0',
        server_ip: serverIp,
        type: 'guest',
        engine: 'threads',
        search_type: 'inject by link',
        search: linkTarget,
        author: THREADS_CRAWLER_AUTHOR,
        account: {
          user: 'guest',
        },
      },
      crawling_time: {
        start: timing.startMs,
        finish: timing.finishMs,
        duration: timing.finishMs - timing.startMs,
      },
      status: 'crawled',
    },
  };
}

function buildThreadsFailedPayload(linkTarget, error, timing, serverIp) {
  return {
    post_url: linkTarget,
    error: String(error || 'Threads data not found'),
    created_time: formatCreatedTime(),
    updated_time: formatCreatedTime(),
    metadata: {
      crawler: {
        version: '1.0.0',
        server_ip: serverIp,
        type: 'guest',
        engine: 'threads',
        search_type: 'inject by link',
        search: linkTarget,
        author: THREADS_CRAWLER_AUTHOR,
        account: {
          user: process.env.THREADS_ACCOUNT_USER || 'guest',
        },
      },
      crawling_time: {
        start: timing.startMs,
        finish: timing.finishMs || null,
        duration: timing.finishMs ? timing.finishMs - timing.startMs : null,
      },
      status: 'data not found',
    },
  };
}

async function sendThreadsFailedPayload(linkTarget, error, timing) {
  if (!THREADS_KAFKA_FAILED_TOPIC) {
    log('WARN', 'KAFKA_TOPIC_THREADS_FAILED is not configured. Threads failed payload not sent.', { url: linkTarget });
    return;
  }

  if (!getKafkaProducer()) {
    log('WARN', 'Kafka producer not connected. Threads failed payload not sent.', { url: linkTarget });
    return;
  }

  try {
    const finishMs = timing.finishMs || Date.now();
    const serverIp = await getPublicIp();
    const payload = buildThreadsFailedPayload(linkTarget, error, { ...timing, finishMs }, serverIp);
    await produceMessage(THREADS_KAFKA_FAILED_TOPIC, linkTarget, payload);
    log('SUCCESS', `Kafka sent Threads failed payload -> [${THREADS_KAFKA_FAILED_TOPIC}]: ${linkTarget}`, { url: linkTarget });
  } catch (e) {
    log('ERROR', `Failed to send Threads failed payload: ${linkTarget}: ${e.message}`, { url: linkTarget });
  }
}

async function fetchThreadsPost(linkTarget) {
  const startMs = Date.now();
  const datetimeCrawlingMs = Math.floor(Date.now() / 1000) * 1000;
  log('INFO', `Fetching Threads: ${linkTarget}`, { url: linkTarget });

  try {
    const cookieHeader = process.env.THREADS_COOKIES || '';
    const response = await axios.get(linkTarget, {
      headers: buildThreadsHeaders(),
      params: THREADS_XMT ? { xmt: THREADS_XMT } : undefined,
      timeout: Number(process.env.THREADS_REQUEST_TIMEOUT_MS || 30000),
      validateStatus: () => true,
      ...(cookieHeader ? { headers: { ...buildThreadsHeaders(), cookie: cookieHeader } } : {}),
    });

    log('INFO', `Threads response ${response.status}`, { url: linkTarget });
    const media = findThreadsMedia(response.data);
    if (!media) throw new Error('Threads media data not found');

    const finishMs = Date.now();
    const serverIp = await getPublicIp();
    const insertData = mapThreadsMedia(media, linkTarget, { startMs, finishMs, datetimeCrawlingMs }, serverIp);

    log('SUCCESS', `Fetched Threads item ${insertData.id || insertData.code}`, { url: linkTarget });
    return { status: 'ok', items: [insertData] };
  } catch (err) {
    log('ERROR', `Failed to fetch Threads post ${linkTarget}: ${err.message}`, { url: linkTarget });
    await sendThreadsFailedPayload(linkTarget, err.message, { startMs, finishMs: Date.now(), datetimeCrawlingMs });
    return {
      status: 'error',
      error: err.message,
      items: [],
    };
  }
}

async function dispatchThreadsItems(items) {
  const results = [];
  const kafkaConnected = !!getKafkaProducer();

  if (!THREADS_KAFKA_TOPIC) {
    log('WARN', 'KAFKA_TOPIC_THREADS is not configured. Threads Kafka dispatch skipped.');
  }

  for (const item of items) {
    const id = item.id || item.code;
    const r = { id, shortcode: id, url: item.__source_url || item.post_url || item.url, kafka: null, mongo: null };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `Threads item skipped: missing id (${r.url || '-'})`);
    } else {
      if (kafkaConnected && THREADS_KAFKA_TOPIC) {
        try {
          await produceMessage(THREADS_KAFKA_TOPIC, id, item);
          log('SUCCESS', `Kafka sent Threads -> [${THREADS_KAFKA_TOPIC}]: ${id}`, { shortcode: id });
          r.kafka = 'sent';
        } catch (e) {
          log('ERROR', `Kafka failed Threads: ${id}: ${e.message}`, { shortcode: id });
          r.kafka = 'failed';
          r.kafkaError = e.message;
        }
      } else {
        r.kafka = kafkaConnected ? 'no_topic' : 'disabled';
      }

      r.mongo = 'disabled';
      log('INFO', `Mongo skipped Threads: ${id} (Kafka only)`, { shortcode: id });
    }

    results.push(r);
    broadcast('dispatch_result', r);
  }

  return results;
}

module.exports = {
  fetchThreadsPost,
  dispatchThreadsItems,
};
