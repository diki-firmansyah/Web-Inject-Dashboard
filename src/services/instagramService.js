'use strict';

const axios = require('axios');
const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { getCookies, getHeaderConfig } = require('../config/state');
const { getKafkaProducer, produceMessage } = require('../config/db');
const {
  accountLabel,
  claimInstagramAccount,
  extractCookiesFromAccount,
  markInstagramAccountSuccess,
  markInstagramAccountFailed,
} = require('./instagramAccountService');

const USE_COOKIES = process.env.USE_COOKIES !== 'false';
const INSTAGRAM_ACCOUNT_POOL_ENABLED = !!(
  process.env.MONGO_USER &&
  process.env.MONGO_PASS &&
  (process.env.MONGO_ACCOUNT_HOST || process.env.MONGO_HOST) &&
  (process.env.MONGO_ACCOUNT_PORT || process.env.MONGO_PORT) &&
  process.env.MONGO_DB_ACCOUNT &&
  process.env.MONGO_COLLECTION_ACCOUNT
);
const INSTAGRAM_CRAWLER_AUTHOR = process.env.INSTAGRAM_CRAWLER_AUTHOR || 'donel';
const INSTAGRAM_CRAWLER_ACCOUNT_USER = process.env.INSTAGRAM_CRAWLER_ACCOUNT_USER || 'logout';

// Kafka topic specifically for Instagram
const INSTAGRAM_KAFKA_TOPIC = process.env.KAFKA_TOPIC_INSTAGRAM;

function cookiesToString(c) {
  return Object.entries(c).map(([k, v]) => `${k}=${v}`).join('; ');
}

function shortcodeFromUrl(url) {
  const m = url.match(/instagram\.com\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/);
  return m ? m[1] : url.trim();
}

function asDict(v) { return (v && typeof v === 'object' && !Array.isArray(v)) ? v : {}; }
function asList(v) { return Array.isArray(v) ? v : []; }
function val(v, fallback = null) { return v === undefined ? fallback : v; }

function formatInstagramWIB(ms) {
  const d = new Date(ms + 7 * 3600_000);
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${days[d.getUTCDay()]} ${months[d.getUTCMonth()]} ${String(d.getUTCDate()).padStart(2, '0')} ${d.getUTCFullYear()} ${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}:${String(d.getUTCSeconds()).padStart(2, '0')} GMT+0700 (Western Indonesia Time)`;
}

function formatCreatedTimeWIB(ms) {
  return new Date(ms + 7 * 3600_000).toISOString().replace('Z', '+07:00');
}

function buildInsertData(edge, shortcode, serverIp, timeStart) {
  const user       = asDict(edge.user);
  const owner      = asDict(edge.owner);
  const caption    = asDict(edge.caption);
  const dimensions = asDict(edge.dimensions);
  const imgVersions = asDict(edge.image_versions2);
  const takenAt    = edge.taken_at || null;

  const displayResources = [];
  const videoVersions = asList(edge.video_versions);
  if (videoVersions.length) {
    const lv = videoVersions.reduce((a, b) =>
      (a.width || 0) * (a.height || 0) >= (b.width || 0) * (b.height || 0) ? a : b);
    displayResources.push({ url: lv.url, width: lv.width, height: lv.height, type: 'video' });
  }
  const imgCandidates = asList(imgVersions.candidates);
  if (imgCandidates.length) {
    const li = imgCandidates.reduce((a, b) =>
      (a.width || 0) * (a.height || 0) >= (b.width || 0) * (b.height || 0) ? a : b);
    displayResources.push({ url: li.url, width: li.width, height: li.height, type: 'image' });
  }

  const datetimeCrawlingMs = Date.now();
  const wib = 7 * 3600_000;
  const dtWIB  = takenAt ? new Date(takenAt * 1000 + wib) : null;
  const dtScrap = new Date(datetimeCrawlingMs + wib);
  const fmtWIB  = (d) => d ? d.toUTCString().replace('GMT', 'GMT+0700 (Western Indonesia Time)') : null;

  const timeFinishMs = Date.now();

  return {
    code:  edge.code  || null,
    pk:    edge.pk    || null,
    id:    edge.id    || null,
    ad_id: edge.ad_id || null,
    boosted_status:              edge.boosted_status              || null,
    boost_unavailable_identifier: edge.boost_unavailable_identifier || null,
    boost_unavailable_reason:    edge.boost_unavailable_reason    || null,
    caption: caption?.text || null,
    caption_is_edited: edge.caption_is_edited || null,
    taken_at: takenAt,
    video_versions:       edge.video_versions       || null,
    is_dash_eligible:     edge.is_dash_eligible     || null,
    number_of_qualities:  edge.number_of_qualities  || null,
    video_dash_manifest:  edge.video_dash_manifest  || null,
    image_versions2:      edge.image_versions2      || null,
    sharing_friction_info: edge.sharing_friction_info || null,
    is_paid_partnership:  edge.is_paid_partnership  || null,
    sponsor_tags:         edge.sponsor_tags         || null,
    original_height:      edge.original_height      || null,
    original_width:       edge.original_width       || null,
    organic_tracking_token: edge.organic_tracking_token || null,
    link:      edge.link      || null,
    story_cta: edge.story_cta || null,
    user: {
      pk: user.pk || null, username: user.username || null,
      profile_pic_url: user.profile_pic_url || null,
      is_private: user.is_private || null, is_verified: user.is_verified || null,
      full_name: user.full_name || null, id: user.id || null,
    },
    owner: {
      pk: owner.pk || null, username: owner.username || null,
      profile_pic_url: owner.profile_pic_url || null,
      is_private: owner.is_private || null, is_verified: owner.is_verified || null,
      full_name: owner.full_name || null, id: owner.id || null,
    },
    comment_count: edge.comment_count || null,
    like_count:    edge.like_count    || null,
    view_count:    edge.view_count    || null,
    video_view_count: edge.video_view_count || null,
    video_play_count: edge.video_play_count || null,
    product_type: edge.product_type || null,
    media_type:   edge.media_type   || null,
    location:     edge.location     || null,
    has_audio:    edge.has_audio    || null,
    carousel_media_count: edge.carousel_media_count || null,
    carousel_media:       edge.carousel_media       || null,
    accessibility_caption: edge.accessibility_caption || null,
    title: edge.title || null,
    is_video: edge.is_video || null,
    is_ad:    edge.is_ad    || null,
    dimensions,
    display_resources: displayResources,
    display_url: edge.display_url || null,
    post_id:   edge.id   || null,
    shortcode: edge.code || null,
    datetime_ms:         takenAt ? takenAt * 1000 : null,
    datetime_ms_scraped: datetimeCrawlingMs,
    datetime_str:         fmtWIB(dtWIB),
    datetime_str_scraped: fmtWIB(dtScrap),
    scrap_type:   'inject by link',
    created_time: dtScrap.toISOString(),
    updated_time: dtScrap.toISOString(),
    version: '3',
    metadata: {
      crawler: {
        server_ip: serverIp, version: '3',
        account: { user: 'cookies', token: null },
        type: 'login', search: shortcode,
        search_type: 'inject by link',
        git_commit_id: null, author: 'webinject-dashboard',
      },
      crawling_time: { start: timeStart, finish: timeFinishMs, duration: timeFinishMs - timeStart },
      status: 'crawled',
    },
  };
}

function enhanceInstagramLegacyOutput(item, edge, shortcode, serverIp, timeStart) {
  const user = asDict(edge.user);
  const owner = asDict(edge.owner);
  const caption = asDict(edge.caption);
  const datetimeCrawlingMs = Date.now();
  const finishMs = Date.now();
  const takenAt = edge.taken_at || null;

  return {
    ...item,
    code: val(edge.code),
    pk: val(edge.pk),
    id: val(edge.id),
    ad_id: val(edge.ad_id),
    boosted_status: val(edge.boosted_status),
    boost_unavailable_identifier: val(edge.boost_unavailable_identifier),
    boost_unavailable_reason: val(edge.boost_unavailable_reason),
    caption: val(caption.text),
    caption_is_edited: val(edge.caption_is_edited),
    feed_demotion_control: val(edge.feed_demotion_control),
    feed_recs_demotion_control: val(edge.feed_recs_demotion_control),
    taken_at: takenAt,
    inventory_source: val(edge.inventory_source),
    affiliate_info: val(edge.affiliate_info),
    user: {
      ...item.user,
      pk: val(user.pk),
      username: val(user.username),
      profile_pic_url: val(user.profile_pic_url),
      is_private: val(user.is_private),
      is_embeds_disabled: val(user.is_embeds_disabled),
      is_unpublished: val(user.is_unpublished),
      is_verified: val(user.is_verified),
      friendship_status: val(user.friendship_status),
      latest_besties_reel_media: val(user.latest_besties_reel_media),
      latest_reel_media: val(user.latest_reel_media),
      live_broadcast_visibility: val(user.live_broadcast_visibility),
      live_broadcast_id: val(user.live_broadcast_id),
      seen: val(user.seen),
      supervision_info: val(user.supervision_info),
      id: val(user.id),
      hd_profile_pic_url_info: val(user.hd_profile_pic_url_info),
      full_name: val(user.full_name),
      __typename: val(user.__typename),
    },
    group: val(edge.group),
    owner: {
      ...item.owner,
      pk: val(user.pk),
      profile_pic_url: val(owner.profile_pic_url),
      username: val(owner.username),
      friendship_status: val(owner.friendship_status),
      is_embeds_disabled: val(owner.is_embeds_disabled),
      is_unpublished: val(owner.is_unpublished),
      is_verified: val(owner.is_verified),
      show_account_transparency_details: val(owner.show_account_transparency_details),
      supervision_info: val(owner.supervision_info),
      transparency_product: val(owner.transparency_product),
      transparency_label: val(owner.transparency_label),
      ai_agent_owner_username: val(owner.ai_agent_owner_username),
      id: val(user.id),
      __typename: val(owner.__typename),
      is_private: val(owner.is_private),
      blocked_by_viewer: val(owner.blocked_by_viewer),
      restricted_by_viewer: val(owner.restricted_by_viewer),
      followed_by_viewer: val(owner.followed_by_viewer),
      full_name: val(user.full_name),
      has_blocked_viewer: val(owner.has_blocked_viewer),
      pass_tiering_recommendation: val(owner.pass_tiering_recommendation),
      edge_owner_to_timeline_media: val(owner.edge_owner_to_timeline_media),
      edge_followed_by: val(owner.edge_followed_by),
      edge_follow: val(owner.edge_follow),
    },
    coauthor_producers: val(edge.coauthor_producers, []),
    invited_coauthor_producers: val(edge.invited_coauthor_producers, []),
    follow_hashtag_info: val(edge.follow_hashtag_info),
    comments_disabled: val(edge.comments_disabled),
    commenting_disabled_for_viewer: val(edge.commenting_disabled_for_viewer),
    like_and_view_counts_disabled: val(edge.like_and_view_counts_disabled),
    has_liked: val(edge.has_liked),
    top_likers: val(edge.top_likers, []),
    facepile_top_likers: val(edge.facepile_top_likers, []),
    preview: val(edge.preview),
    can_see_insights_as_brand: val(edge.can_see_insights_as_brand),
    can_reshare: val(edge.can_reshare),
    can_viewer_reshare: val(edge.can_viewer_reshare),
    ig_media_sharing_disabled: val(edge.ig_media_sharing_disabled),
    photo_of_you: val(edge.photo_of_you),
    usertags: val(edge.usertags),
    media_overlay_info: val(edge.media_overlay_info),
    carousel_parent_id: val(edge.carousel_parent_id),
    clips_metadata: val(edge.clips_metadata),
    clips_attribution_info: val(edge.clips_attribution_info),
    audience: val(edge.audience),
    display_uri: val(edge.display_uri),
    media_cropping_info: val(edge.media_cropping_info),
    profile_grid_thumbnail_fitting_style: val(edge.profile_grid_thumbnail_fitting_style),
    thumbnails: val(edge.thumbnails),
    timeline_pinned_user_ids: val(edge.timeline_pinned_user_ids, []),
    upcoming_event: val(edge.upcoming_event),
    logging_info_token: val(edge.logging_info_token),
    explore: val(edge.explore),
    main_feed_carousel_starting_media_id: val(edge.main_feed_carousel_starting_media_id),
    is_seen: val(edge.is_seen),
    open_carousel_submission_state: val(edge.open_carousel_submission_state),
    previous_submitter: val(edge.previous_submitter),
    all_previous_submitters: val(edge.all_previous_submitters),
    headline: val(edge.headline),
    comments: val(edge.comments),
    fb_like_count: val(edge.fb_like_count),
    saved_collection_ids: val(edge.saved_collection_ids),
    has_viewer_saved: val(edge.has_viewer_saved),
    media_level_comment_controls: val(edge.media_level_comment_controls),
    __typename: val(edge.__typename),
    video_src: val(edge.video_src),
    base_64_pic: val(edge.base_64_pic),
    edge_media_preview_comment: asDict(edge.edge_media_preview_comment),
    edge_media_preview_like: asDict(edge.edge_media_preview_like),
    edge_media_to_caption: asDict(edge.edge_media_to_caption),
    edge_media_to_comment: asDict(edge.edge_media_to_comment),
    edge_media_to_hoisted_comment: asDict(edge.edge_media_to_hoisted_comment),
    edge_media_to_parent_comment: asDict(edge.edge_media_to_parent_comment),
    edge_media_to_sponsor_user: asDict(edge.edge_media_to_sponsor_user),
    edge_media_to_tagged_user: asDict(edge.edge_media_to_tagged_user),
    edge_related_profiles: asDict(edge.edge_related_profiles),
    edge_sidecar_to_children: asDict(edge.edge_sidecar_to_children),
    edge_web_media_to_related_media: asDict(edge.edge_web_media_to_related_media),
    fact_check_information: val(edge.fact_check_information),
    fact_check_overall_rating: val(edge.fact_check_overall_rating),
    gating_info: val(edge.gating_info),
    has_ranked_comments: val(edge.has_ranked_comments),
    hashtag_search: val(edge.hashtag_search),
    media_preview: val(edge.media_preview),
    sensitivity_friction_info: asDict(edge.sensitivity_friction_info),
    taken_at_timestamp: val(edge.taken_at_timestamp),
    video_url: val(edge.video_url),
    viewer_can_reshare: val(edge.viewer_can_reshare),
    viewer_has_liked: val(edge.viewer_has_liked),
    viewer_has_saved: val(edge.viewer_has_saved),
    viewer_has_saved_to_collection: val(edge.viewer_has_saved_to_collection),
    viewer_in_photo_of_you: val(edge.viewer_in_photo_of_you),
    organic_tracking_token: val(edge.organic_tracking_token),
    datetime_ms: takenAt ? takenAt * 1000 : null,
    datetime_ms_scraped: datetimeCrawlingMs,
    datetime_str: takenAt ? formatInstagramWIB(takenAt * 1000) : null,
    datetime_str_scraped: formatInstagramWIB(datetimeCrawlingMs),
    created_time: formatCreatedTimeWIB(datetimeCrawlingMs),
    updated_time: formatCreatedTimeWIB(datetimeCrawlingMs),
    metadata: {
      crawler: {
        server_ip: serverIp,
        version: '3',
        account: {
          user: INSTAGRAM_CRAWLER_ACCOUNT_USER,
          token: null,
        },
        type: 'login',
        search: shortcode,
        search_type: 'inject by link',
        git_commit_id: process.env.GIT_COMMIT_ID || null,
        author: INSTAGRAM_CRAWLER_AUTHOR,
      },
      crawling_time: {
        start: timeStart,
        finish: finishMs,
        duration: finishMs - timeStart,
      },
      status: 'crawled',
    },
  };
}

async function fetchShortcode(shortcode) {
  const timeStart = Date.now();
  log('INFO', `▶ Fetching: ${shortcode}`, { shortcode });

  const claimedAccount = await claimInstagramAccount();
  if (INSTAGRAM_ACCOUNT_POOL_ENABLED && !claimedAccount) {
    return { shortcode, status: 'no_account', error: 'No available Instagram account', items: [] };
  }

  const accountCookies = claimedAccount ? extractCookiesFromAccount(claimedAccount) : {};
  if (claimedAccount && !Object.keys(accountCookies).length) {
    await markInstagramAccountFailed(claimedAccount, 'Cookie tidak ditemukan di dokumen akun', shortcode);
    return { shortcode, status: 'account_cookie_missing', error: 'Cookie akun tidak ditemukan', items: [] };
  }
  if (claimedAccount && !accountCookies.sessionid) {
    await markInstagramAccountFailed(claimedAccount, 'sessionid tidak ditemukan di cookie akun', shortcode);
    return { shortcode, status: 'account_session_missing', error: 'sessionid akun tidak ditemukan', items: [] };
  }

  const currentCookies = Object.keys(accountCookies).length ? accountCookies : getCookies();
  const baseConfig = getHeaderConfig();
  const cfg = {
    ...baseConfig,
    csrftoken: currentCookies.csrftoken || baseConfig.csrftoken,
  };

  if (claimedAccount) {
    log('INFO', `Instagram using account: ${accountLabel(claimedAccount)}`, { shortcode });
  }

  const vars = { shortcode, __relay_internal__pv__PolarisShareSheetV3relayprovider: false };
  const form = new URLSearchParams({
    av:'0', __d:'www', __user:'0', __a:'1', __req:'2',
    __hs:'20314.HYP:instagram_web_pkg.2.1...0', dpr:'2', __ccg:'GOOD',
    __rev:'1025892304', __s:'hu24b5:rcqkb8:layzgc', __hsi:'7538416601183641333',
    __dyn:'7xeUjG1mxu1syUbFp41twpUnwgU7SbzEdF8aUco2qwJw5ux609vCwjE1EE2Cw8G11wBz81s8hwGxu786a3a1YwBgao6C0Mo2swtUd8-U2zxe2GewGw9a361qw8Xxm16wa-0raazo7u3C2u2J0bS1LwTwKG0WE8oC1Iwqo5p0OwUQp1yU426V89F8uwm8jwhU6W1tyVrx60gm5oswFwtF8',
    __comet_req:'7',
    lsd: cfg.x_fb_lsd, jazoest:'21012',
    __spin_r:'1025892304', __spin_b:'trunk',
    __spin_t: String(Math.floor(Date.now() / 1000)),
    __crn:'comet.igweb.PolarisLoggedOutDesktopPostRouteNext',
    fb_api_caller_class:'RelayModern',
    fb_api_req_friendly_name:'PolarisPostRootQuery',
    variables: JSON.stringify(vars),
    server_timestamps:'true',
    doc_id: cfg.doc_id,
  });

  const headers = {
    accept: '*/*',
    'accept-language': 'en-US,en;q=0.9',
    origin: 'https://www.instagram.com',
    priority: 'u=1, i',
    referer: `https://www.instagram.com/p/${shortcode}/`,
    'sec-ch-prefers-color-scheme': 'dark',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': cfg.user_agent,
    'x-asbd-id': cfg.x_asbd_id,
    'x-bloks-version-id': cfg.x_bloks_version_id,
    'x-csrftoken': cfg.csrftoken,
    'x-fb-friendly-name': 'PolarisPostRootQuery',
    'x-fb-lsd': cfg.x_fb_lsd,
    'x-ig-app-id': cfg.x_ig_app_id,
    'x-root-field-name': 'xdt_api__v1__media__shortcode__web_info',
    'content-type': 'application/x-www-form-urlencoded',
  };

  const cookieStr = cookiesToString(currentCookies);
  if (USE_COOKIES && cookieStr) {
    headers['cookie'] = cookieStr;
  }
  if (USE_COOKIES && !currentCookies.sessionid) {
    log('WARN', 'Instagram cookie login belum lengkap: sessionid tidak ditemukan. Update cookie dari browser yang sudah login.', { shortcode });
  }
  if (!USE_COOKIES) {
    log('WARN', 'USE_COOKIES=false, Instagram request berjalan sebagai guest/logout.', { shortcode });
  }

  let response;
  try {
    response = await axios.post('https://www.instagram.com/graphql/query', form.toString(), { headers, timeout: 20000 });
  } catch (err) {
    const msg = err.response ? `HTTP ${err.response.status}` : err.message;
    await markInstagramAccountFailed(claimedAccount, msg, shortcode);
    log('ERROR', `✗ Request failed for ${shortcode}: ${msg}`, { shortcode });
    return { shortcode, status: 'request_failed', error: msg, items: [] };
  }

  log('INFO', `  Response ${response.status} (${JSON.stringify(response.data).length} bytes)`, { shortcode });
  if (response.status !== 200) {
    log('ERROR', `✗ Non-200 for ${shortcode}`, { shortcode });
    await markInstagramAccountFailed(claimedAccount, `HTTP ${response.status}`, shortcode);
    return { shortcode, status: 'http_error', error: response.status, items: [] };
  }

  const rawData  = response.data;
  const edges    = asList(asDict(asDict(rawData?.data)?.xdt_api__v1__media__shortcode__web_info)?.items);

  if (!edges.length) {
    const firstError = asList(rawData?.errors)[0];
    if (firstError?.code === 1675030 || firstError?.summary === 'Query Error') {
      log('WARN', `Instagram Query Error for ${shortcode}. Biasanya cookie/header login sudah expired atau doc_id/header fingerprint perlu diperbarui.`, { shortcode });
    }
    log('WARN', `✗ No items for ${shortcode}. Preview: ${JSON.stringify(rawData).slice(0, 400)}`, { shortcode });
    await markInstagramAccountFailed(claimedAccount, firstError?.summary || firstError?.message || 'No items returned', shortcode);
    return { shortcode, status: 'no_data', items: [] };
  }

  let serverIp = 'Unable to determine IP';
  try {
    const ip = await axios.get('https://api.ipify.org?format=json', { timeout: 4000 });
    serverIp = ip.data?.ip || 'Unable to determine IP';
  } catch (_) {}

  const items = edges.map(edge => enhanceInstagramLegacyOutput(buildInsertData(edge, shortcode, serverIp, timeStart), edge, shortcode, serverIp, timeStart));
  await markInstagramAccountSuccess(claimedAccount, shortcode);
  log('SUCCESS', `✓ Fetched ${items.length} item(s) for ${shortcode}`, { shortcode });
  return { shortcode, status: 'ok', items };
}

async function dispatchItems(items) {
  const results = [];
  const kafkaConnected = !!getKafkaProducer();

  for (const item of items) {
    const id = item.id;
    const r = { id, shortcode: item.shortcode || id, url: item.__source_url || item.url || item.post_url, kafka: null, mongo: null };

    if (!id) {
      r.kafka = 'skipped';
      r.mongo = 'skipped';
      r.skipReason = 'missing_id';
      log('WARN', `Instagram item skipped: missing id (${r.url || r.shortcode || '-'})`);
    } else {
      // Kafka dispatch
      if (kafkaConnected) {
        try {
          await produceMessage(INSTAGRAM_KAFKA_TOPIC, item.id, item);
          log('SUCCESS', `✓ Kafka sent: ${item.shortcode} (id=${item.id})`, { shortcode: item.shortcode });
          r.kafka = 'sent';
        } catch (e) {
          log('ERROR', `✗ Kafka failed: ${item.shortcode}: ${e.message}`, { shortcode: item.shortcode });
          r.kafka = 'failed';
          r.kafkaError = e.message;
        }
      } else {
        r.kafka = 'disabled';
      }

      r.mongo = 'disabled';
      log('INFO', `Mongo skipped Instagram: ${item.shortcode} (Kafka only)`, { shortcode: item.shortcode });
    }

    results.push(r);
    broadcast('dispatch_result', r);
  }
  return results;
}

module.exports = {
  shortcodeFromUrl,
  fetchShortcode,
  dispatchItems
};
