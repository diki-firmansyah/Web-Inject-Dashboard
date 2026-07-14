'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchTwitterPost, dispatchTwitterItems } = require('../services/twitterService');

module.exports = createSocialController({
  label: 'Twitter/X',
  fetchPost: fetchTwitterPost,
  dispatchItems: dispatchTwitterItems,
});
