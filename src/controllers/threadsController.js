'use strict';

const { createSocialController } = require('./socialControllerFactory');
const { fetchThreadsPost, dispatchThreadsItems } = require('../services/threadsService');

module.exports = createSocialController({
  label: 'Threads',
  fetchPost: fetchThreadsPost,
  dispatchItems: dispatchThreadsItems,
});
