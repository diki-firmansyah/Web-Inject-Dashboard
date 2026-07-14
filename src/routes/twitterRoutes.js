'use strict';

const express = require('express');
const router = express.Router();
const twitterController = require('../controllers/twitterController');

router.post('/twitter/crawl', twitterController.crawl);
router.post('/twitter/send', twitterController.send);
router.post('/twitter/crawl-and-send', twitterController.crawlAndSend);

module.exports = router;
