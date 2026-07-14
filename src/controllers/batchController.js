'use strict';

const { createJob, getJob, listJobs, pauseJob, stopJob, resumeJob, findPausedJob } = require('../services/batchQueueService');

function startBatch(req, res) {
  try {
    if (req.body?.resumePaused) {
      const paused = findPausedJob(req.body.engine);
      if (paused) {
        const job = resumeJob(paused.id);
        return res.status(202).json({ ok: true, resumed: true, job });
      }
    }

    const job = createJob(req.body || {});
    res.status(202).json({ ok: true, job });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
}

function batchStatus(req, res) {
  const job = getJob(req.params.id);
  if (!job) return res.status(404).json({ ok: false, error: 'Batch job not found' });
  res.json({ ok: true, job });
}

function batchList(_req, res) {
  res.json({ ok: true, jobs: listJobs() });
}

function pauseBatch(req, res) {
  const job = pauseJob(req.params.id);
  if (!job) return res.status(404).json({ ok: false, error: 'Batch job not found' });
  res.json({ ok: true, job });
}

function resumeBatch(req, res) {
  const job = resumeJob(req.params.id);
  if (!job) return res.status(404).json({ ok: false, error: 'Batch job not found' });
  res.json({ ok: true, job });
}

function stopBatch(req, res) {
  const job = stopJob(req.params.id);
  if (!job) return res.status(404).json({ ok: false, error: 'Batch job not found' });
  res.json({ ok: true, job });
}

module.exports = {
  startBatch,
  batchStatus,
  batchList,
  pauseBatch,
  resumeBatch,
  stopBatch,
};
