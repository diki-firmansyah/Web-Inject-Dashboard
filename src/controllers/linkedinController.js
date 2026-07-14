'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchLinkedinPost, dispatchLinkedinItems } = require('../services/linkedinService');

module.exports = createSocialController({
  label: 'LinkedIn',
  fetchPost: fetchLinkedinPost,
  dispatchItems: dispatchLinkedinItems,
});
