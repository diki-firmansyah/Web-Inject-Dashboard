'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchYouTubePost, dispatchYouTubeItems } = require('../services/youtubeService');

module.exports = createSocialController({
  label: 'YouTube',
  fetchPost: fetchYouTubePost,
  dispatchItems: dispatchYouTubeItems,
});
