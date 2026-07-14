'use strict';

const express = require('express');
const router = express.Router();
const threadsController = require('../controllers/threadsController');

router.post('/threads/crawl', threadsController.crawl);
router.post('/threads/send', threadsController.send);
router.post('/threads/crawl-and-send', threadsController.crawlAndSend);

module.exports = router;
