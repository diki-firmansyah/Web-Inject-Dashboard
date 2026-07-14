'use strict';

const express = require('express');
const router = express.Router();
const tiktokController = require('../controllers/tiktokController');

router.post('/tiktok/crawl', tiktokController.crawl);
router.post('/tiktok/send', tiktokController.send);
router.post('/tiktok/crawl-and-send', tiktokController.crawlAndSend);

module.exports = router;
