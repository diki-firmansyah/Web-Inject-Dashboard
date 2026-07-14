'use strict';

const { log } = require('../utils/logger');
const { broadcast } = require('../utils/sse');
const { shortcodeFromUrl, fetchShortcode, dispatchItems } = require('./instagramService');
const { fetchTikTokPost, dispatchTikTokItems } = require('./tiktokService');
const { fetchTwitterPost, dispatchTwitterItems } = require('./twitterService');
const { fetchYouTubePost, dispatchYouTubeItems } = require('./youtubeService');
const { fetchThreadsPost, dispatchThreadsItems } = require('./threadsService');
const { fetchLinkedinPost, dispatchLinkedinItems } = require('./linkedinService');
const { fetchFacebookPost, dispatchFacebookItems } = require('./facebookService');

const BATCH_DELAY_MIN_MS = Number(process.env.BATCH_DELAY_MIN_MS || 2000);
const BATCH_DELAY_MAX_MS = Number(process.env.BATCH_DELAY_MAX_MS || 5000);
const TWITTER_BATCH_DELAY_MIN_MS = Number(process.env.TWITTER_BATCH_DELAY_MIN_MS || 15000);
const TWITTER_BATCH_DELAY_MAX_MS = Number(process.env.TWITTER_BATCH_DELAY_MAX_MS || 30000);

const engines = {
  instagram: {
    label: 'Instagram',
    fetchPost: target => fetchShortcode(shortcodeFromUrl(target)),
    dispatchItems,
  },
  tiktok: {
    label: 'TikTok',
    fetchPost: fetchTikTokPost,
    dispatchItems: dispatchTikTokItems,
  },
  twitter: {
    label: 'Twitter/X',
    fetchPost: fetchTwitterPost,
    dispatchItems: dispatchTwitterItems,
  },
  youtube: {
    label: 'YouTube',
    fetchPost: fetchYouTubePost,
    dispatchItems: dispatchYouTubeItems,
  },
  threads: {
    label: 'Threads',
    fetchPost: fetchThreadsPost,
    dispatchItems: dispatchThreadsItems,
  },
  linkedin: {
    label: 'LinkedIn',
    fetchPost: fetchLinkedinPost,
    dispatchItems: dispatchLinkedinItems,
  },
  facebook: {
    label: 'Facebook',
    fetchPost: fetchFacebookPost,
    dispatchItems: dispatchFacebookItems,
  },
};

const jobs = new Map();
const queues = Object.fromEntries(Object.keys(engines).map(engine => [engine, []]));
const runningWorkers = Object.fromEntries(Object.keys(engines).map(engine => [engine, false]));

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function randomBetween(min, max) {
  if (max <= min) return min;
  return min + Math.floor(Math.random() * (max - min + 1));
}

function nextDelayMs(engine) {
  if (engine === 'twitter') {
    return randomBetween(TWITTER_BATCH_DELAY_MIN_MS, TWITTER_BATCH_DELAY_MAX_MS);
  }

  return randomBetween(BATCH_DELAY_MIN_MS, BATCH_DELAY_MAX_MS);
}

function publicJob(job) {
  const crawlErrors = Array.isArray(job.errors) ? job.errors : [];
  const sendErrors = Array.isArray(job.sendErrors) ? job.sendErrors : [];

  return {
    id: job.id,
    engine: job.engine,
    label: job.label,
    status: job.status,
    total: job.total,
    processed: job.processed,
    success: job.success,
    failed: job.failed,
    currentUrl: job.currentUrl,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    updatedAt: job.updatedAt,
    itemsSent: job.itemsSent,
    pauseRequested: job.pauseRequested,
    stopRequested: job.stopRequested,
    errors: crawlErrors.slice(-20),
    failedTargets: [...crawlErrors, ...sendErrors],
    sendFailed: sendErrors.length,
    recentResults: job.results.slice(-20),
  };
}

function enqueueJob(job) {
  if (!queues[job.engine]) queues[job.engine] = [];
  if (!queues[job.engine].includes(job.id)) queues[job.engine].push(job.id);
  runWorker(job.engine);
}

function emitJob(job) {
  broadcast('batch_update', publicJob(job));
}

function createJob({ engine, urls, autoSend = true }) {
  const cfg = engines[engine];
  if (!cfg) {
    const supported = Object.keys(engines).join(', ');
    throw new Error(`Engine tidak dikenal: ${engine}. Supported: ${supported}`);
  }

  const normalizedUrls = [...new Set((urls || []).map(url => String(url || '').trim()).filter(Boolean))];
  if (!normalizedUrls.length) throw new Error('No URLs provided');

  const now = new Date().toISOString();
  const job = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    engine,
    label: cfg.label,
    autoSend: autoSend !== false,
    urls: normalizedUrls,
    status: 'queued',
    total: normalizedUrls.length,
    processed: 0,
    success: 0,
    failed: 0,
    itemsSent: 0,
    pauseRequested: false,
    stopRequested: false,
    currentUrl: null,
    results: [],
    errors: [],
    sendErrors: [],
    createdAt: now,
    startedAt: null,
    finishedAt: null,
    updatedAt: now,
  };

  jobs.set(job.id, job);
  log('INFO', `Batch queued ${cfg.label}: ${job.total} target(s). Job ${job.id}`);
  emitJob(job);
  enqueueJob(job);
  return publicJob(job);
}

async function processJob(job) {
  const cfg = engines[job.engine];
  job.status = 'running';
  if (!job.startedAt) job.startedAt = new Date().toISOString();
  job.finishedAt = null;
  job.updatedAt = job.startedAt;
  log('INFO', `Batch started ${job.label}: ${job.total} target(s). Job ${job.id}`);
  emitJob(job);

  for (let index = job.processed; index < job.urls.length; index++) {
    if (job.stopRequested) {
      job.status = 'stopped';
      job.currentUrl = null;
      job.finishedAt = new Date().toISOString();
      job.updatedAt = job.finishedAt;
      log('WARN', `Batch stopped ${job.label}: ${job.processed}/${job.total}. Job ${job.id}`);
      emitJob(job);
      return;
    }

    if (job.pauseRequested) {
      job.status = 'paused';
      job.currentUrl = null;
      job.updatedAt = new Date().toISOString();
      log('WARN', `Batch paused ${job.label}: ${job.processed}/${job.total}. Job ${job.id}`);
      emitJob(job);
      return;
    }

    const url = job.urls[index];
    job.currentUrl = url;
    job.updatedAt = new Date().toISOString();
    emitJob(job);

    try {
      const crawlResult = await cfg.fetchPost(url);
      const items = crawlResult?.items || [];

      if (crawlResult?.status === 'ok' && items.length > 0) {
        let dispatchResults = [];
        if (job.autoSend) {
          items.forEach(item => {
            if (item && typeof item === 'object') {
              Object.defineProperty(item, '__source_url', {
                value: url,
                enumerable: false,
                configurable: true,
              });
            }
          });
          dispatchResults = await cfg.dispatchItems(items);
          dispatchResults.forEach(result => {
            if (result && !result.url) result.url = url;
          });
          dispatchResults
            .filter(result => result?.kafka === 'failed' || result?.mongo === 'failed')
            .forEach(result => {
              const message = result.kafkaError || result.mongoError || 'Send failed';
              job.sendErrors.push({ url: result.url || url, error: message, phase: 'send' });
              log('WARN', `Batch ${job.label} send failed: ${result.url || url} - ${message}`);
            });
          job.itemsSent += dispatchResults.filter(result =>
            ['sent', 'saved', 'updated', 'exists'].includes(result?.kafka) ||
            ['sent', 'saved', 'updated', 'exists'].includes(result?.mongo)
          ).length;
        }

        job.success += 1;
        job.results.push({
          url,
          status: 'success',
          itemCount: items.length,
          dispatchCount: dispatchResults.length,
        });
      } else {
        job.failed += 1;
        const message = crawlResult?.error || `Tidak ada item berhasil di-crawl`;
        job.errors.push({ url, error: message, phase: 'crawl' });
        job.results.push({ url, status: 'failed', error: message });
        log('WARN', `Batch ${job.label} target failed: ${url} - ${message}`);
      }
    } catch (e) {
      job.failed += 1;
      job.errors.push({ url, error: e.message, phase: 'crawl' });
      job.results.push({ url, status: 'failed', error: e.message });
      log('ERROR', `Batch ${job.label} target error: ${url}: ${e.message}`);
    }

    job.processed += 1;
    job.updatedAt = new Date().toISOString();
    emitJob(job);

    if (job.stopRequested) {
      job.status = 'stopped';
      job.currentUrl = null;
      job.finishedAt = new Date().toISOString();
      job.updatedAt = job.finishedAt;
      log('WARN', `Batch stopped ${job.label}: ${job.processed}/${job.total}. Job ${job.id}`);
      emitJob(job);
      return;
    }

    if (job.processed < job.total) {
      const sleepMs = nextDelayMs(job.engine);
      log('INFO', `Batch ${job.label} sleeping ${sleepMs}ms before next target...`);
      await delay(sleepMs);
    }
  }

  job.status = job.failed > 0 ? (job.success > 0 ? 'completed_with_errors' : 'failed') : 'completed';
  job.currentUrl = null;
  job.finishedAt = new Date().toISOString();
  job.updatedAt = job.finishedAt;
  log('SUCCESS', `Batch finished ${job.label}: Total ${job.total} | Berhasil ${job.success} | Gagal ${job.failed} | Sent ${job.itemsSent}. Job ${job.id}`);
  emitJob(job);
}

async function runWorker(engine) {
  if (!queues[engine]) queues[engine] = [];
  if (runningWorkers[engine]) return;
  runningWorkers[engine] = true;

  try {
    while (queues[engine].length > 0) {
      const jobId = queues[engine].shift();
      const job = jobs.get(jobId);
      if (!job || job.status !== 'queued') continue;
      await processJob(job);
    }
  } finally {
    runningWorkers[engine] = false;
  }
}

function pauseJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return null;
  if (!['queued', 'running'].includes(job.status)) return publicJob(job);

  job.pauseRequested = true;
  if (job.status === 'queued') {
    job.status = 'paused';
    job.currentUrl = null;
  }
  job.updatedAt = new Date().toISOString();
  log('WARN', `Batch pause requested ${job.label}: Job ${job.id}`);
  emitJob(job);
  return publicJob(job);
}

function stopJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return null;
  if (['completed', 'completed_with_errors', 'failed', 'stopped'].includes(job.status)) return publicJob(job);

  job.stopRequested = true;
  job.pauseRequested = false;

  if (job.status === 'queued' || job.status === 'paused') {
    job.status = 'stopped';
    job.currentUrl = null;
    job.finishedAt = new Date().toISOString();
  } else if (job.status === 'running') {
    job.status = 'stopping';
  }

  job.updatedAt = new Date().toISOString();
  log('WARN', `Batch stop requested ${job.label}: Job ${job.id}`);
  emitJob(job);
  return publicJob(job);
}

function resumeJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return null;
  if (job.status !== 'paused') return publicJob(job);

  job.pauseRequested = false;
  job.stopRequested = false;
  job.status = 'queued';
  job.updatedAt = new Date().toISOString();
  log('INFO', `Batch resumed ${job.label}: ${job.processed}/${job.total}. Job ${job.id}`);
  emitJob(job);
  enqueueJob(job);
  return publicJob(job);
}

function findPausedJob(engine) {
  return Array.from(jobs.values())
    .reverse()
    .find(job => job.engine === engine && job.status === 'paused') || null;
}

function getJob(jobId) {
  const job = jobs.get(jobId);
  return job ? publicJob(job) : null;
}

function listJobs() {
  return Array.from(jobs.values()).slice(-50).reverse().map(publicJob);
}

module.exports = {
  createJob,
  getJob,
  listJobs,
  pauseJob,
  stopJob,
  resumeJob,
  findPausedJob,
};
