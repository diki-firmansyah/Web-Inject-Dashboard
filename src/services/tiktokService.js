'use strict';

const { chromium } = require('playwright');
const cheerio = require('cheerio');
const axios = require('axios');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getKafkaProducer, produceMessage } = require('../config/db');

// Kafka topic specifically for TikTok
const TIKTOK_KAFKA_TOPIC = process.env.KAFKA_TOPIC_TIKTOK;
const TIKTOK_CRAWLER_AUTHOR = process.env.TIKTOK_CRAWLER_AUTHOR || 'donel';

function cleanUrl(url) {
  let u = url.trim();
  if (u.includes('/photo/')) {
    u = u.replace('/photo/', '/video/');
  }
  return u;
}

function mapTiktokPost(data, postUrl, msToken, nowMs, serverIp) {
  const postId = data.id;
  const desc = data.desc || '';
  const bookmarkCount = data.stats ? data.stats.collectCount : 0;
  const commentCount = data.stats ? data.stats.commentCount : 0;
  const postLikeCount = data.stats ? data.stats.diggCount : 0;
  const viewCount = data.stats ? data.stats.playCount : 0;
  const shareCount = data.stats ? data.stats.shareCount : 0;

  const videoUrl = data.video ? data.video.playAddr : null;
  const imageUrl = data.video ? data.video.cover : null;

  const musicTitle = data.music ? data.music.title : '';
  const formattedMusicTitle = musicTitle.trim().replace(/"/g, '').replace(/\s+/g, '-');
  const musicId = data.music ? data.music.id : '';
  const musicUrl = `https://www.tiktok.com/music/${formattedMusicTitle}-${musicId}`;

  // Owner
  const authorId = data.author ? data.author.id : '';
  const secuid = data.author ? data.author.secUid : '';
  const username = data.author ? data.author.uniqueId : '';
  const nickname = data.author ? data.author.nickname : '';
  const profilePicUrl = data.author ? data.author.avatarMedium : '';
  const followers = data.authorStats ? data.authorStats.followerCount : 0;
  const following = data.authorStats ? data.authorStats.followingCount : 0;
  const likeCount = data.authorStats ? data.authorStats.heartCount : 0;
  const videoCount = data.authorStats ? data.authorStats.videoCount : 0;
  const diggCount = data.authorStats ? data.authorStats.diggCount : 0;

  // Post Date
  const createdTime = parseInt(data.createTime || 0);
  const createdDt = new Date(createdTime * 1000);
  
  const now = new Date();
  const diffSec = Math.floor((now - createdDt) / 1000);
  let timeText = '';
  if (diffSec < 60) timeText = `${diffSec} seconds ago`;
  else if (diffSec < 3600) timeText = `${Math.floor(diffSec / 60)} minutes ago`;
  else if (diffSec < 86400) timeText = `${Math.floor(diffSec / 3600)} hours ago`;
  else timeText = `${Math.floor(diffSec / 86400)} days ago`;

  const postDate = {
    text: timeText,
    datetime: createdDt.toISOString().replace(/\.\d+Z$/, 'Z'),
    timestamp: createdTime,
    datetimems: createdTime * 1000
  };

  const crawlingFinishMs = Date.now();
  const crawlingDurationMs = crawlingFinishMs - nowMs;

  const metadata = {
    crawler: {
      server_ip: serverIp,
      git_commit_id: process.env.GIT_COMMIT_ID || null,
      account: {
        user: "guest",
        token: msToken || null
      },
      type: "non-login",
      search: username,
      search_type: "tiktok-posts-inject",
      author: TIKTOK_CRAWLER_AUTHOR
    },
    crawling_time: {
      start: nowMs,
      finish: crawlingFinishMs,
      duration: crawlingDurationMs
    }
  };

  const insertData = {
    post_id: postId,
    auto_generate_desc: {
      description: "Log in or sign up for an account on TikTok. Start watching to discover real people and real videos that will make your day.",
      "og:description": desc,
      "twitter:description": desc
    },
    datetime_crawling_ms: nowMs,
    datetime_ms: createdTime * 1000,
    desc: {
      text: desc,
      html: ""
    },
    bookmark_count: {
      text: String(bookmarkCount),
      number: bookmarkCount
    },
    comment_count: {
      text: String(commentCount),
      number: commentCount
    },
    like_count: {
      text: String(postLikeCount),
      number: postLikeCount
    },
    view_count: {
      text: String(viewCount),
      number: viewCount
    },
    share_count: {
      text: String(shareCount),
      number: shareCount
    },
    media: {
      image: {
        url: imageUrl
      },
      video: {
        blob_video_url: null,
        video_url: videoUrl
      }
    },
    meta: {
      keyword: username,
      post_url: postUrl
    },
    music: {
      url: musicUrl,
      title: musicTitle
    },
    owner: {
      id: authorId,
      secuid: secuid,
      username: username,
      nickname: nickname,
      profile_pic_url: profilePicUrl,
      followers: followers,
      following: following,
      like_count: likeCount,
      video_count: videoCount,
      digg_count: diggCount
    },
    post_date: postDate,
    metadata: metadata,
    video_title: "Log in | TikTok"
  };

  return { postId, insertData };
}

async function fetchTikTokPost(postUrl) {
  const nowMs = Date.now();
  const targetUrl = cleanUrl(postUrl);
  log('INFO', `▶ Fetching TikTok: ${targetUrl}`, { url: postUrl });

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
      locale: "en-US"
    });
    const page = await context.newPage();

    await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 150000 });
    try {
      await page.waitForLoadState('networkidle', { timeout: 30000 });
    } catch (e) {
      log('WARN', `Timeout waiting for networkidle on ${targetUrl}, proceeding anyway.`);
    }
    await page.waitForTimeout(5000);

    const html = await page.content();
    const cookies = await context.cookies();
    const cookieDict = {};
    for (const c of cookies) {
      cookieDict[c.name] = c.value;
    }
    const msToken = cookieDict["msToken"];
    await browser.close();

    const $ = cheerio.load(html);
    const script = $('#__UNIVERSAL_DATA_FOR_REHYDRATION__');
    if (!script.length) {
      throw new Error("Element __UNIVERSAL_DATA_FOR_REHYDRATION__ not found in page HTML");
    }

    const jsonText = script.text().trim();
    const resHtml = JSON.parse(jsonText);

    let data;
    try {
      data = resHtml["__DEFAULT_SCOPE__"]["webapp.video-detail"]["itemInfo"]["itemStruct"];
    } catch (e) {
      throw new Error("Failed to traverse webapp.video-detail.itemInfo.itemStruct in Universal Data");
    }

    let serverIp = null;
    try {
      const ip = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
      serverIp = ip.data?.ip || null;
    } catch (_) {}

    const { postId, insertData } = mapTiktokPost(data, postUrl, msToken, nowMs, serverIp);
    log('SUCCESS', `✓ Fetched TikTok item ${postId} successfully`, { url: postUrl });
    return { status: 'ok', items: [insertData] };

  } catch (err) {
    if (browser) await browser.close();
    log('ERROR', `✗ Failed to fetch TikTok post ${postUrl}: ${err.message}`, { url: postUrl });
    return { status: 'error', error: err.message, items: [] };
  }
}

async function dispatchTikTokItems(items) {
  const results = [];
  const kafkaConnected = !!getKafkaProducer();

  if (!TIKTOK_KAFKA_TOPIC) {
    log('WARN', '⚠️ KAFKA_TOPIC_TIKTOK belum dikonfigurasi di .env — TikTok Kafka dispatch dilewati.');
  }

  for (const item of items) {
    const id = item.post_id;
    const r = { id, shortcode: id, url: item.__source_url || item.url || item.post_url, kafka: null, mongo: null };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `TikTok item skipped: missing id (${r.url || '-'})`);
    } else {
      // Kafka dispatch — menggunakan topic tiktok_post (KAFKA_TOPIC_TIKTOK)
      if (kafkaConnected && TIKTOK_KAFKA_TOPIC) {
        try {
          await produceMessage(TIKTOK_KAFKA_TOPIC, id, item);
          log('SUCCESS', `✓ Kafka sent TikTok → [${TIKTOK_KAFKA_TOPIC}]: ${item.post_id}`, { shortcode: item.post_id });
          r.kafka = 'sent';
        } catch (e) {
          log('ERROR', `✗ Kafka failed TikTok: ${item.post_id}: ${e.message}`, { shortcode: item.post_id });
          r.kafka = 'failed';
          r.kafkaError = e.message;
        }
      } else {
        r.kafka = kafkaConnected ? 'no_topic' : 'disabled';
      }

      r.mongo = 'disabled';
      log('INFO', `Mongo skipped TikTok: ${item.post_id} (Kafka only)`, { shortcode: item.post_id });
    }

    results.push(r);
    broadcast('dispatch_result', r);
  }
  return results;
}

module.exports = {
  fetchTikTokPost,
  dispatchTikTokItems
};
