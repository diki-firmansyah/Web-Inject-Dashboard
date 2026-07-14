'use strict';

const express = require('express');
const router = express.Router();
const statusController = require('../controllers/statusController');

router.get('/status', statusController.checkStatus);
router.get('/stream', statusController.streamEvents);

module.exports = router;
