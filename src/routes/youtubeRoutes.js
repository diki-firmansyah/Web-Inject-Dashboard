'use strict';

const express = require('express');
const router = express.Router();
const youtubeController = require('../controllers/youtubeController');

router.post('/youtube/crawl', youtubeController.crawl);
router.post('/youtube/send', youtubeController.send);
router.post('/youtube/crawl-and-send', youtubeController.crawlAndSend);

module.exports = router;
