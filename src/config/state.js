'use strict';

function parseCookieString(cookieString) {
  return String(cookieString || '')
    .split(';')
    .map(part => part.trim())
    .filter(Boolean)
    .reduce((acc, part) => {
      const index = part.indexOf('=');
      if (index === -1) return acc;
      const key = part.slice(0, index).trim();
      const value = part.slice(index + 1).trim();
      if (key) acc[key] = value;
      return acc;
    }, {});
}

const defaultSessionCookies = {
  csrftoken:  'Yso_eZrmjaScgGOnO960wY',
  datr:       '5tWdaD9cImplKItkniAEReDN',
  ig_did:     '1BAEDFCD-E950-4439-8BAE-559BA8C87D93',
  mid:        'aJ3V5wAEAAEMshIYUsmb9PSr0L__',
  ig_nrcb:    '1',
  wd:         '1440x380',
};

let sessionCookies = {
  ...defaultSessionCookies,
  ...parseCookieString(process.env.INSTAGRAM_COOKIES),
};

let headerConfig = {
  csrftoken:          'Yso_eZrmjaScgGOnO960wY',
  x_fb_lsd:           'AVq0unDobsQ',
  x_ig_app_id:        '936619743392459',
  doc_id:             '24760146316904293',
  x_bloks_version_id: '4fd52d0e0985dd463fefe21d18f1609258ecf3c799cc7f12f6c4363b56697384',
  x_asbd_id:          '359341',
  user_agent:         'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
};

if (sessionCookies.csrftoken) {
  headerConfig.csrftoken = sessionCookies.csrftoken;
}

function getCookies() {
  return sessionCookies;
}

function updateCookies(newCookies) {
  sessionCookies = { ...newCookies };
  return sessionCookies;
}

function getHeaderConfig() {
  return headerConfig;
}

function updateHeaderConfig(newConfig) {
  headerConfig = { ...headerConfig, ...newConfig };
  return headerConfig;
}

module.exports = {
  getCookies,
  updateCookies,
  getHeaderConfig,
  updateHeaderConfig,
  parseCookieString
};
