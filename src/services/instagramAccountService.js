'use strict';

const { ObjectId } = require('mongodb');
const { getInstagramAccountCollection } = require('../config/db');
const { parseCookieString } = require('../config/state');
const { log } = require('../utils/logger');

const COOKIE_KEYS = [
  'sessionid',
  'ds_user_id',
  'csrftoken',
  'mid',
  'ig_did',
  'datr',
  'rur',
  'wd',
  'ig_nrcb',
];

function accountId(account) {
  return account?._id instanceof ObjectId ? account._id.toHexString() : String(account?._id || '');
}

function accountLabel(account) {
  return account?.username || account?.user || account?.account || accountId(account) || 'unknown';
}

function normalizeCookieValue(value) {
  if (!value) return {};
  if (typeof value === 'string') return parseCookieString(value);
  if (typeof value === 'object' && !Array.isArray(value)) return value;
  if (Array.isArray(value)) {
    return value.reduce((acc, item) => {
      if (item?.name && item?.value) acc[item.name] = item.value;
      return acc;
    }, {});
  }
  return {};
}

function extractCookiesFromAccount(account) {
  const sources = [
    account?.cookies,
    account?.cookie,
    account?.cookie_string,
    account?.cookies_string,
    account?.raw_cookie,
    account?.instagram_cookies,
    account?.session?.cookies,
    account?.auth?.cookies,
    account?.data?.cookies,
  ];

  for (const source of sources) {
    const cookies = normalizeCookieValue(source);
    if (Object.keys(cookies).length) return cookies;
  }

  const rootCookies = {};
  for (const key of COOKIE_KEYS) {
    if (account?.[key]) rootCookies[key] = account[key];
  }
  return rootCookies;
}

async function claimInstagramAccount() {
  const collection = getInstagramAccountCollection();
  if (!collection) {
    log('WARN', 'Instagram account Mongo belum terkoneksi. Fallback ke cookie lokal/env.');
    return null;
  }

  const now = new Date();
  const result = await collection.findOneAndUpdate(
    { available: true, in_use: false },
    {
      $set: {
        in_use: true,
        last_claimed_at: now,
        updated_at: now,
      },
      $inc: { claim_count: 1 },
    },
    {
      sort: { last_success_at: 1, updated_at: 1, _id: 1 },
      returnDocument: 'after',
    }
  );

  const account = result?.value || result;
  if (!account) {
    log('WARN', 'Tidak ada akun Instagram available=true dan in_use=false di MongoDB.');
    return null;
  }

  log('INFO', `Instagram account claimed: ${accountLabel(account)}`);
  return account;
}

async function markInstagramAccountSuccess(account, shortcode) {
  const collection = getInstagramAccountCollection();
  if (!collection || !account?._id) return;

  const now = new Date();
  await collection.updateOne(
    { _id: account._id },
    {
      $set: {
        available: true,
        in_use: true,
        last_success_at: now,
        last_shortcode: shortcode || null,
        last_error: null,
        updated_at: now,
      },
      $inc: { success_count: 1 },
    }
  );
  log('SUCCESS', `Instagram account success: ${accountLabel(account)}`);
}

async function markInstagramAccountFailed(account, reason, shortcode) {
  const collection = getInstagramAccountCollection();
  if (!collection || !account?._id) return;

  const now = new Date();
  await collection.updateOne(
    { _id: account._id },
    {
      $set: {
        available: false,
        in_use: false,
        last_failed_at: now,
        last_shortcode: shortcode || null,
        last_error: String(reason || 'Instagram crawl failed').slice(0, 500),
        updated_at: now,
      },
      $inc: { failed_count: 1 },
    }
  );
  log('WARN', `Instagram account disabled: ${accountLabel(account)} - ${reason || 'crawl failed'}`);
}

module.exports = {
  accountLabel,
  claimInstagramAccount,
  extractCookiesFromAccount,
  markInstagramAccountSuccess,
  markInstagramAccountFailed,
};
