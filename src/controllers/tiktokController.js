'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchTikTokPost, dispatchTikTokItems } = require('../services/tiktokService');

module.exports = createSocialController({
  label: 'TikTok',
  fetchPost: fetchTikTokPost,
  dispatchItems: dispatchTikTokItems,
});
