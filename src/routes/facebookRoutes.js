'use strict';

const express = require('express');
const router = express.Router();
const facebookController = require('../controllers/facebookController');

router.post('/facebook/crawl', facebookController.crawl);
router.post('/facebook/send', facebookController.send);
router.post('/facebook/crawl-and-send', facebookController.crawlAndSend);

module.exports = router;
