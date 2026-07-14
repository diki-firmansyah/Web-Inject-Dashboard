'use strict';

const express = require('express');
const router = express.Router();
const linkedinController = require('../controllers/linkedinController');

router.post('/linkedin/crawl', linkedinController.crawl);
router.post('/linkedin/send', linkedinController.send);
router.post('/linkedin/crawl-and-send', linkedinController.crawlAndSend);

module.exports = router;
