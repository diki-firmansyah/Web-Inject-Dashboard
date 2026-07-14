'use strict';

const axios = require('axios');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getMongoCollection } = require('../config/db');

const YOUTUBE_REQUEST_TIMEOUT_MS = Number(process.env.YOUTUBE_REQUEST_TIMEOUT_MS || 30000);
const YOUTUBE_USER_AGENT = process.env.YOUTUBE_USER_AGENT || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36';

function extractYouTubeId(url) {
  const value = String(url || '').trim();
  const patterns = [
    /(?:v=|\/videos\/|embed\/|youtu\.be\/|\/watch\?v=|\/shorts\/)([A-Za-z0-9_-]{11})/i,
  ];

  for (const pattern of patterns) {
    const match = value.match(pattern);
    if (match) return match[1];
  }

  return null;
}

function extractChannelUsernameFromUrl(value) {
  if (!value) return '';

  try {
    const parsed = new URL(value);
    const parts = parsed.pathname.split('/').filter(Boolean);
    const handle = parts.find(part => part.startsWith('@'));
    if (handle) return handle;

    const channelIndex = parts.findIndex(part => part.toLowerCase() === 'channel');
    if (channelIndex >= 0 && parts[channelIndex + 1]) return parts[channelIndex + 1];

    const userIndex = parts.findIndex(part => part.toLowerCase() === 'user' || part.toLowerCase() === 'c');
    if (userIndex >= 0 && parts[userIndex + 1]) return parts[userIndex + 1];
  } catch (_) {
    const handleMatch = String(value).match(/\/(@[^/?#]+)/);
    if (handleMatch) return handleMatch[1];
  }

  return '';
}

function extractChannelUsernameFromHtml(html) {
  const patterns = [
    /"ownerProfileUrl":"([^"]+)"/,
    /"canonicalChannelUrl":"([^"]+)"/,
    /<link[^>]+itemprop=["']url["'][^>]+href=["']([^"']+)["']/i,
    /<meta[^>]+property=["']og:url["'][^>]+content=["']([^"']+)["']/i,
  ];

  for (const pattern of patterns) {
    const match = String(html || '').match(pattern);
    if (!match) continue;

    const decoded = match[1].replace(/\\u0026/g, '&').replace(/\\\//g, '/');
    const username = extractChannelUsernameFromUrl(decoded);
    if (username) return username;
  }

  return '';
}

async function fetchYouTubeChannelUsername(videoId, videoUrl) {
  const normalizedUrl = `https://www.youtube.com/watch?v=${videoId}`;

  try {
    const oembed = await axios.get('https://www.youtube.com/oembed', {
      params: { url: normalizedUrl, format: 'json' },
      timeout: YOUTUBE_REQUEST_TIMEOUT_MS,
      headers: { 'user-agent': YOUTUBE_USER_AGENT },
    });

    const username = extractChannelUsernameFromUrl(oembed.data?.author_url);
    if (username) return username;
  } catch (e) {
    log('WARN', `YouTube oEmbed channel lookup failed: ${videoId}: ${e.message}`, { shortcode: videoId });
  }

  try {
    const page = await axios.get(videoUrl || normalizedUrl, {
      timeout: YOUTUBE_REQUEST_TIMEOUT_MS,
      headers: {
        'user-agent': YOUTUBE_USER_AGENT,
        accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,id;q=0.8',
      },
    });

    return extractChannelUsernameFromHtml(page.data);
  } catch (e) {
    log('WARN', `YouTube page channel lookup failed: ${videoId}: ${e.message}`, { shortcode: videoId });
    return '';
  }
}

async function buildYouTubeListPayload(videoUrl) {
  const videoId = extractYouTubeId(videoUrl);
  if (!videoId) return null;

  const channelUsername = await fetchYouTubeChannelUsername(videoId, videoUrl);
  if (channelUsername) {
    log('SUCCESS', `YouTube channel_username extracted: ${videoId} -> ${channelUsername}`, { shortcode: videoId });
  } else {
    log('WARN', `YouTube channel_username kosong: ${videoId}`, { shortcode: videoId });
  }

  return {
    vid: videoId,
    channel_username: channelUsername,
    is_crawled: false,
    search_query: 'inject by url',
    search_type: 'inject by url',
  };
}

async function fetchYouTubePost(videoUrl) {
  log('INFO', `YouTube URL inject: ${videoUrl}`, { url: videoUrl });

  const insertData = await buildYouTubeListPayload(videoUrl);
  if (!insertData) {
    const message = `Tidak bisa ekstrak video ID dari URL: ${videoUrl}`;
    log('ERROR', message, { url: videoUrl });
    return { status: 'error', error: message, items: [] };
  }

  log('SUCCESS', `YouTube video id extracted: ${insertData.vid}`, { url: videoUrl });
  return { status: 'ok', items: [insertData] };
}

async function dispatchYouTubeItems(items) {
  const results = [];
  const youtubeDbName = process.env.MONGO_DB_NAME;
  const youtubeCollectionName = process.env.MONGO_COLLECTION_LIST || 'youtube_list';
  const mongoCollection = getMongoCollection(youtubeCollectionName, youtubeDbName);

  for (const item of items) {
    const id = item.vid;
    const r = { id, shortcode: id, url: item.__source_url || item.url || (id ? `https://www.youtube.com/watch?v=${id}` : null), kafka: 'disabled', mongo: null };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `YouTube item skipped: missing id (${r.url || '-'})`);
    } else {
      log('INFO', `Kafka skipped YouTube: ${id} (MongoDB only)`, { shortcode: id });

      if (mongoCollection) {
        try {
          const existing = await mongoCollection.findOne({ vid: id });
          if (existing) {
            if (!existing.channel_username && item.channel_username) {
              await mongoCollection.updateOne({ vid: id }, { $set: item });
              log('SUCCESS', `MongoDB updated YouTube channel_username: ${id} -> ${item.channel_username}`, { shortcode: id });
              r.mongo = 'updated';
            } else {
              log('WARN', `YouTube data sudah tercrawling sebelumnya: ${id} (${youtubeDbName}.${youtubeCollectionName})`, { shortcode: id });
              r.mongo = 'exists';
            }
          } else {
            await mongoCollection.updateOne({ vid: id }, { $set: item }, { upsert: true });
            log('SUCCESS', `MongoDB upserted YouTube: ${id} -> ${youtubeDbName}.${youtubeCollectionName}`, { shortcode: id });
            r.mongo = 'saved';
          }
        } catch (e) {
          log('ERROR', `Mongo failed YouTube: ${id}: ${e.message}`, { shortcode: id });
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

module.exports = {
  fetchYouTubePost,
  dispatchYouTubeItems,
};
