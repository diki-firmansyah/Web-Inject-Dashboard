'use strict';

const axios = require('axios');
const { createPageCrawlerService, parseTwitter } = require('./webPageSocialService');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getKafkaProducer } = require('../config/db');

const DEFAULT_TWITTER_BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA';
const TWITTER_GRAPHQL_429_BACKOFF_MS = Number(process.env.TWITTER_GRAPHQL_429_BACKOFF_MS || 60000);

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function extractTweetId(url) {
  const m = String(url).match(/(?:x|twitter)\.com\/[^/]+\/status\/(\d+)/i);
  return m ? m[1] : String(url || '').trim();
}

function extractTwitterAuthor(url, _title, authorName) {
  const m = String(url).match(/(?:x|twitter)\.com\/([^/]+)\/status\//i);
  const username = m ? m[1] : null;
  return {
    id: null,
    username,
    name: authorName || username,
    profileUrl: username ? `https://x.com/${username}` : null,
  };
}

function parseCookieHeader(cookieHeader) {
  const cookieDict = {};
  for (const part of String(cookieHeader || '').split(';')) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const index = trimmed.indexOf('=');
    if (index === -1) continue;
    cookieDict[trimmed.slice(0, index)] = trimmed.slice(index + 1);
  }
  return cookieDict;
}

async function getPublicIp() {
  try {
    const response = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
    return response.data?.ip || null;
  } catch (e) {
    log('WARN', `Failed to fetch IP address. Reason: ${e.message}`);
    return null;
  }
}

function defaultTweetDetailVariables(tweetId) {
  return {
    focalTweetId: String(tweetId),
    with_rux_injections: false,
    rankingMode: 'Relevance',
    includePromotedContent: true,
    withCommunity: true,
    withQuickPromoteEligibilityTweetFields: true,
    withBirdwatchNotes: true,
    withVoice: true,
  };
}

function defaultTweetDetailFeatures() {
  return {
    rweb_video_screen_enabled: false,
    payments_enabled: false,
    profile_label_improvements_pcf_label_in_post_enabled: true,
    rweb_tipjar_consumption_enabled: true,
    verified_phone_label_enabled: false,
    creator_subscriptions_tweet_preview_api_enabled: true,
    responsive_web_graphql_timeline_navigation_enabled: true,
    responsive_web_graphql_skip_user_profile_image_extensions_enabled: false,
    premium_content_api_read_enabled: false,
    communities_web_enable_tweet_community_results_fetch: true,
    c9s_tweet_anatomy_moderator_badge_enabled: true,
    responsive_web_grok_analyze_button_fetch_trends_enabled: false,
    responsive_web_grok_analyze_post_followups_enabled: true,
    responsive_web_jetfuel_frame: false,
    responsive_web_grok_share_attachment_enabled: true,
    articles_preview_enabled: true,
    responsive_web_edit_tweet_api_enabled: true,
    graphql_is_translatable_rweb_tweet_is_translatable_enabled: true,
    view_counts_everywhere_api_enabled: true,
    longform_notetweets_consumption_enabled: true,
    responsive_web_twitter_article_tweet_consumption_enabled: true,
    tweet_awards_web_tipping_enabled: false,
    responsive_web_grok_show_grok_translated_post: false,
    responsive_web_grok_analysis_button_from_backend: true,
    creator_subscriptions_quote_tweet_preview_enabled: false,
    freedom_of_speech_not_reach_fetch_enabled: true,
    standardized_nudges_misinfo: true,
    tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled: true,
    longform_notetweets_rich_text_read_enabled: true,
    longform_notetweets_inline_media_enabled: true,
    responsive_web_grok_image_annotation_enabled: true,
    responsive_web_grok_community_note_auto_translation_is_enabled: false,
    responsive_web_enhance_cards_enabled: false,
  };
}

function parseJsonEnv(name, fallback) {
  if (!process.env[name]) return fallback;
  try {
    return JSON.parse(process.env[name]);
  } catch (e) {
    log('WARN', `${name} is not valid JSON. Using default. Reason: ${e.message}`);
    return fallback;
  }
}

function findTweetResult(root, tweetId) {
  let found = null;

  const entries = root?.data?.threaded_conversation_with_injections_v2?.instructions
    ?.flatMap(instruction => instruction.entries || []) || [];
  const targetEntry = entries.find(entry => entry.entryId === `tweet-${tweetId}`);
  const targetResult = targetEntry?.content?.itemContent?.tweet_results?.result;
  if (targetResult?.legacy) return targetResult.core ? targetResult : targetResult.tweet;

  function walk(value) {
    if (found || !value || typeof value !== 'object') return;

    const result = value.result || value.tweetResult?.result;
    const legacy = result?.legacy || value.legacy;
    if (legacy?.id_str === String(tweetId) && (legacy.full_text || legacy.created_at)) {
      found = result || value;
      return;
    }

    if (Array.isArray(value)) {
      for (const item of value) walk(item);
      return;
    }

    for (const item of Object.values(value)) walk(item);
  }

  walk(root);
  return found;
}

function getUserResult(tweetResult) {
  return tweetResult?.core?.user_results?.result || tweetResult?.core?.user_result?.result || null;
}

function toNumber(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toTwitterNumberId(idStr) {
  if (!idStr || !/^\d+$/.test(String(idStr))) return null;
  const n = Number(idStr);
  return Number.isSafeInteger(n) ? n : String(idStr);
}

function twitterDateToMs(createdAt) {
  const ms = Date.parse(createdAt);
  return Number.isFinite(ms) ? ms : null;
}

function createdTimeNow() {
  return new Date().toISOString().replace('Z', '000');
}

function mapUser(userResult) {
  const legacy = userResult?.legacy || {};
  const core = userResult?.core || {};
  const privacy = userResult?.privacy || {};
  const verification = userResult?.verification || {};
  const locationData = userResult?.location || {};
  const avatar = userResult?.avatar || {};
  const idStr = userResult?.rest_id || legacy.id_str || null;
  const screenName = core.screen_name || legacy.screen_name || null;
  const profileImage = avatar.image_url || legacy.profile_image_url_https || legacy.profile_image_url || null;

  return {
    id: toTwitterNumberId(idStr),
    id_str: idStr,
    name: core.name || legacy.name || null,
    screen_name: screenName,
    location: locationData.location || legacy.location || null,
    url: screenName ? `https://x.com/${screenName}` : null,
    description: legacy.description || null,
    translator_type: legacy.translator_type || 'none',
    protected: privacy.protected ?? legacy.protected ?? null,
    verified: verification.verified ?? legacy.verified ?? userResult?.is_blue_verified ?? false,
    followers_count: toNumber(legacy.followers_count),
    friends_count: toNumber(legacy.friends_count),
    listed_count: toNumber(legacy.listed_count),
    favourites_count: toNumber(legacy.favourites_count),
    statuses_count: toNumber(legacy.statuses_count),
    created_at: core.created_at || legacy.created_at || null,
    utc_offset: null,
    time_zone: null,
    geo_enabled: legacy.geo_enabled ?? null,
    lang: legacy.lang ?? null,
    contributors_enabled: legacy.contributors_enabled ?? null,
    is_translator: legacy.is_translator ?? false,
    profile_background_color: legacy.profile_background_color ?? null,
    profile_background_image_url: legacy.profile_background_image_url ?? null,
    profile_background_image_url_https: legacy.profile_background_image_url_https ?? null,
    profile_background_tile: legacy.profile_background_tile ?? null,
    profile_link_color: legacy.profile_link_color ?? null,
    profile_sidebar_border_color: legacy.profile_sidebar_border_color ?? null,
    profile_sidebar_fill_color: legacy.profile_sidebar_fill_color ?? null,
    profile_text_color: legacy.profile_text_color ?? null,
    profile_use_background_image: legacy.profile_use_background_image ?? null,
    profile_image_url: profileImage,
    profile_image_url_https: profileImage,
    profile_banner_url: legacy.profile_banner_url || null,
    default_profile: legacy.default_profile ?? false,
    default_profile_image: legacy.default_profile_image ?? false,
    following: null,
    follow_request_sent: null,
    notifications: null,
  };
}

function mapQuotedStatus(tweetResult) {
  const quoted = tweetResult?.quoted_status_result?.result;
  if (!quoted?.legacy) return null;
  return mapTweetResultToKafka(quoted, {
    tweetId: quoted.legacy.id_str,
    serverIp: null,
    cookieHeader: null,
    csrfToken: null,
    startMs: Date.now(),
    includeMetadata: false,
  });
}

function mapTweetResultToKafka(tweetResult, options) {
  const legacy = tweetResult?.legacy || {};
  const userResult = getUserResult(tweetResult);
  const createdAt = legacy.created_at || null;
  const datetimeMs = twitterDateToMs(createdAt);
  const idStr = legacy.id_str || options.tweetId || null;
  const finishMs = Date.now();
  const quotedStatus = mapQuotedStatus(tweetResult);
  const quotedStatusId = legacy.quoted_status_id_str || quotedStatus?.id_str || null;

  const payload = {
    created_at: createdAt,
    id: toTwitterNumberId(idStr),
    id_str: idStr,
    text: legacy.full_text || legacy.text || null,
    source: tweetResult?.source || legacy.source || null,
    truncated: legacy.truncated ?? false,
    in_reply_to_status_id: toTwitterNumberId(legacy.in_reply_to_status_id_str),
    in_reply_to_status_id_str: legacy.in_reply_to_status_id_str || null,
    in_reply_to_user_id: toTwitterNumberId(legacy.in_reply_to_user_id_str),
    in_reply_to_user_id_str: legacy.in_reply_to_user_id_str || null,
    in_reply_to_screen_name: legacy.in_reply_to_screen_name || null,
    user: mapUser(userResult),
    geo: legacy.geo || null,
    coordinates: legacy.coordinates || null,
    place: legacy.place || null,
    contributors: legacy.contributors || null,
    quoted_status_id: toTwitterNumberId(quotedStatusId),
    quoted_status_id_str: quotedStatusId,
    quoted_status: quotedStatus,
    quoted_status_permalink: {
      url: null,
      expanded: null,
      display: null,
    },
    is_quote_status: legacy.is_quote_status ?? false,
    quote_count: toNumber(legacy.quote_count),
    reply_count: toNumber(legacy.reply_count),
    retweet_count: toNumber(legacy.retweet_count),
    favorite_count: toNumber(legacy.favorite_count),
    bookmark_count: toNumber(legacy.bookmark_count),
    views: toNumber(tweetResult?.views?.count),
    entities: legacy.entities || {},
    favorited: legacy.favorited ?? false,
    retweeted: legacy.retweeted ?? false,
    filter_level: null,
    lang: null,
    timestamp_ms: datetimeMs ? Math.floor(datetimeMs / 1000) : null,
    datetime_str: createdAt,
    datetime_ms: datetimeMs,
    keyword_search: null,
  };

  if (options.includeMetadata !== false) {
    payload.metadata = {
      crawler: {
        server_ip: options.serverIp,
        git_commit_id: process.env.GIT_COMMIT_ID || null,
        account: {
          user: null,
          token: options.cookieHeader,
          x_csrf_token: options.csrfToken,
          verified: false,
        },
        type: options.cookieHeader ? 'login' : 'guest',
        search: idStr,
        lang: null,
        page: null,
        search_type: 'inject-post-url',
        client_id: null,
        author: process.env.TWITTER_CRAWLER_AUTHOR || 'webinject-dashboard',
      },
      crawling_time: {
        start: options.startMs,
        finish: finishMs,
        duration: finishMs - options.startMs,
      },
    };
  }

  payload.created_time = createdTimeNow();
  payload.updated_time = null;

  return payload;
}

function buildTweetDetailParams(tweetId) {
  const variables = {
    ...defaultTweetDetailVariables(tweetId),
    ...parseJsonEnv('TWITTER_GRAPHQL_VARIABLES_JSON', {}),
    focalTweetId: String(tweetId),
  };

  return {
    variables: JSON.stringify(variables),
    features: JSON.stringify({
      ...defaultTweetDetailFeatures(),
      ...parseJsonEnv('TWITTER_GRAPHQL_FEATURES_JSON', {}),
    }),
    fieldToggles: JSON.stringify(parseJsonEnv('TWITTER_GRAPHQL_FIELD_TOGGLES_JSON', {
      withArticleRichContentState: true,
      withArticlePlainText: false,
      withGrokAnalyze: false,
      withDisallowedReplyControls: false,
    })),
  };
}

function hasGraphqlConfig() {
  return Boolean(process.env.TWITTER_QUERY_ID && process.env.TWITTER_COOKIES);
}

async function fetchTwitterViaGraphql(rawUrl) {
  const startMs = Date.now();
  const tweetId = extractTweetId(rawUrl);
  const queryId = process.env.TWITTER_QUERY_ID;
  const cookieHeader = process.env.TWITTER_COOKIES;
  const cookieDict = parseCookieHeader(cookieHeader);
  const csrfToken = process.env.TWITTER_X_CSRF_TOKEN || cookieDict.ct0 || '';
  const bearerToken = process.env.TWITTER_BEARER_TOKEN || DEFAULT_TWITTER_BEARER_TOKEN;
  const reqUrl = `https://x.com/i/api/graphql/${queryId}/TweetDetail`;

  log('INFO', `Fetching Twitter/X GraphQL TweetDetail: ${tweetId}`, { url: rawUrl });

  const headers = {
    accept: '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    cookie: cookieHeader,
    referer: String(rawUrl).startsWith('http') ? rawUrl : `https://x.com/i/status/${tweetId}`,
    'user-agent': process.env.TWITTER_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'x-csrf-token': csrfToken,
    'x-twitter-active-user': 'yes',
    'x-twitter-auth-type': 'OAuth2Session',
    'x-twitter-client-language': process.env.TWITTER_CLIENT_LANGUAGE || 'en',
  };

  if (bearerToken) headers.authorization = bearerToken.startsWith('Bearer ') ? bearerToken : `Bearer ${bearerToken}`;
  if (process.env.TWITTER_X_CLIENT_TRANSACTION_ID) {
    headers['x-client-transaction-id'] = process.env.TWITTER_X_CLIENT_TRANSACTION_ID;
  }

  const response = await axios.get(reqUrl, {
    headers,
    params: buildTweetDetailParams(tweetId),
    timeout: Number(process.env.TWITTER_REQUEST_TIMEOUT_MS || 30000),
  });

  const tweetResult = findTweetResult(response.data, tweetId);
  if (!tweetResult) throw new Error('Tweet result not found in GraphQL response');

  const serverIp = await getPublicIp();
  const item = mapTweetResultToKafka(tweetResult, {
    tweetId,
    serverIp,
    cookieHeader,
    csrfToken,
    startMs,
  });

  log('SUCCESS', `Fetched Twitter/X GraphQL item ${item.id_str}`, { url: rawUrl });
  return { status: 'ok', items: [item] };
}

const fallbackService = createPageCrawlerService({
  platform: 'twitter',
  label: 'Twitter/X',
  collectionEnv: 'MONGO_COLLECTION_TWITTER',
  collectionDefault: 'twitter_inject_by_link',
  topicEnv: 'KAFKA_TOPIC_TWITTER',
  extractId: extractTweetId,
  extractAuthor: extractTwitterAuthor,
  parse: parseTwitter,
});

async function fetchTwitterPost(rawUrl) {
  if (!hasGraphqlConfig()) {
    log('WARN', 'TWITTER_QUERY_ID/TWITTER_COOKIES belum lengkap. Fallback ke web metadata crawler.');
    return fallbackService.fetchPost(rawUrl);
  }

  try {
    return await fetchTwitterViaGraphql(rawUrl);
  } catch (e) {
    const status = e.response?.status;
    if (status === 429) {
      log('WARN', `Twitter/X GraphQL rate limited (429). Backoff ${TWITTER_GRAPHQL_429_BACKOFF_MS}ms sebelum fallback.`, { url: rawUrl });
      await delay(TWITTER_GRAPHQL_429_BACKOFF_MS);
    } else {
      log('ERROR', `Twitter/X GraphQL failed: ${e.message}. Fallback ke web metadata crawler.`, { url: rawUrl });
    }

    if (status === 429) {
      log('WARN', 'Twitter/X GraphQL fallback ke web metadata crawler setelah backoff.', { url: rawUrl });
    }
    return fallbackService.fetchPost(rawUrl);
  }
}

function stringifyTwitterKafkaPayload(item) {
  const json = JSON.stringify(item);
  const idStr = item?.id_str;
  if (!idStr || !/^\d+$/.test(String(idStr))) return json;

  // Preserve the legacy Python Kafka shape: top-level `id` is an unquoted
  // integer literal, even when it exceeds JavaScript's safe integer range.
  return json.replace(/"id":(?:"[^"]+"|\d+)/, `"id":${idStr}`);
}

async function dispatchTwitterItems(items) {
  const results = [];
  const topicName = process.env.KAFKA_TOPIC_TWITTER;
  const kafkaProducer = getKafkaProducer();

  if (!topicName) {
    log('WARN', 'KAFKA_TOPIC_TWITTER is not configured. Twitter/X Kafka dispatch skipped.');
  }

  for (const item of items) {
    const id = item.id_str || item.id;
    const r = { id, shortcode: id, url: item.__source_url || item.url || item.post_url, kafka: null, mongo: null };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `Twitter/X item skipped: missing id (${r.url || '-'})`);
    } else {
      if (kafkaProducer && topicName) {
        try {
          await kafkaProducer.send({
            topic: topicName,
            messages: [{ key: String(id), value: stringifyTwitterKafkaPayload(item) }],
          });
          log('SUCCESS', `Kafka sent Twitter/X -> [${topicName}]: ${id}`, { shortcode: id });
          r.kafka = 'sent';
        } catch (e) {
          log('ERROR', `Kafka failed Twitter/X: ${id}: ${e.message}`, { shortcode: id });
          r.kafka = 'failed';
          r.kafkaError = e.message;
        }
      } else {
        r.kafka = kafkaProducer ? 'no_topic' : 'disabled';
      }

      r.mongo = 'disabled';
      log('INFO', `Mongo skipped Twitter/X: ${id} (Kafka only)`, { shortcode: id });
    }

    results.push(r);
    broadcast('dispatch_result', r);
  }

  return results;
}

module.exports = {
  fetchTwitterPost,
  dispatchTwitterItems,
};
