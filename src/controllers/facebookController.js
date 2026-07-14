'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchFacebookPost, dispatchFacebookItems } = require('../services/facebookService');

module.exports = createSocialController({
  label: 'Facebook',
  fetchPost: fetchFacebookPost,
  dispatchItems: dispatchFacebookItems,
});
