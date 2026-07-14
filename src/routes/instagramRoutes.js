'use strict';

const express = require('express');
const router = express.Router();
const instagramController = require('../controllers/instagramController');

router.post('/crawl', instagramController.crawl);
router.post('/send', instagramController.send);
router.post('/crawl-and-send', instagramController.crawlAndSend);

router.get('/cookies', instagramController.getCookies);
router.post('/cookies', instagramController.updateCookies);

router.get('/config', instagramController.getConfig);
router.post('/config', instagramController.updateConfig);

module.exports = router;
