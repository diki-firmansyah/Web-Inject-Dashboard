'use strict';

const express = require('express');
const router = express.Router();
const batchController = require('../controllers/batchController');

router.post('/batch', batchController.startBatch);
router.get('/batch', batchController.batchList);
router.get('/batch/:id', batchController.batchStatus);
router.post('/batch/:id/pause', batchController.pauseBatch);
router.post('/batch/:id/resume', batchController.resumeBatch);
router.post('/batch/:id/stop', batchController.stopBatch);

module.exports = router;
