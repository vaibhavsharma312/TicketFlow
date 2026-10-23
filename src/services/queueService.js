// src/services/queueService.js
//
// A virtual waiting room. When a sale opens, letting every user hit the
// booking endpoint at once just moves the stampede onto the seat rows.
// Instead, users take a numbered token and only the first N are admitted
// to book; the rest poll for their position.
//
// This is the pattern Ticketmaster / BookMyShow use for high-demand sales.
// Enabled with QUEUE_ENABLED=true so it never gets in the way of load tests.

const crypto = require('crypto');
const cache = require('../config/cache');
const logger = require('../config/logger');

const CAPACITY = parseInt(process.env.QUEUE_CAPACITY, 10) || 50;   // concurrent admits
const TOKEN_TTL_SECONDS = parseInt(process.env.QUEUE_TOKEN_TTL, 10) || 300;

const WAITING_KEY = 'queue:waiting';
const ADMITTED_PREFIX = 'queue:admitted:';

// In-memory fallback: insertion-ordered map of token -> joinedAt
const memWaiting = new Map();
const memAdmitted = new Map(); // token -> expiresAt

function isEnabled() {
  return process.env.QUEUE_ENABLED === 'true';
}

function newToken() {
  return crypto.randomBytes(12).toString('hex');
}

function pruneMemory() {
  const now = Date.now();
  for (const [t, exp] of memAdmitted) if (exp < now) memAdmitted.delete(t);
  for (const [t, joined] of memWaiting) {
    if (now - joined > TOKEN_TTL_SECONDS * 1000) memWaiting.delete(t);
  }
}

async function join() {
  const token = newToken();
  const redis = cache.raw();

  if (redis) {
    await redis.zAdd(WAITING_KEY, { score: Date.now(), value: token });
  } else {
    pruneMemory();
    memWaiting.set(token, Date.now());
  }

  const status = await getStatus(token);
  logger.info({ token: token.slice(0, 8), position: status.position }, 'queue join');
  return { token, ...status };
}

async function getStatus(token) {
  const redis = cache.raw();

  if (redis) {
    if (await redis.get(ADMITTED_PREFIX + token)) {
      return { admitted: true, position: 0, waiting: 0 };
    }
    const rank = await redis.zRank(WAITING_KEY, token);
    if (rank === null) return { admitted: false, position: null, expired: true, waiting: 0 };

    const total = await redis.zCard(WAITING_KEY);
    const admittedCount = (await redis.keys(ADMITTED_PREFIX + '*')).length;

    if (rank < CAPACITY - admittedCount) {
      await redis.zRem(WAITING_KEY, token);
      await redis.set(ADMITTED_PREFIX + token, '1', { EX: TOKEN_TTL_SECONDS });
      return { admitted: true, position: 0, waiting: total - 1 };
    }
    return { admitted: false, position: rank + 1, waiting: total };
  }

  // memory mode
  pruneMemory();
  if (memAdmitted.has(token)) return { admitted: true, position: 0, waiting: memWaiting.size };

  const tokens = [...memWaiting.keys()];
  const idx = tokens.indexOf(token);
  if (idx === -1) return { admitted: false, position: null, expired: true, waiting: 0 };

  if (idx < CAPACITY - memAdmitted.size) {
    memWaiting.delete(token);
    memAdmitted.set(token, Date.now() + TOKEN_TTL_SECONDS * 1000);
    return { admitted: true, position: 0, waiting: memWaiting.size };
  }
  return { admitted: false, position: idx + 1, waiting: memWaiting.size };
}

async function isAdmitted(token) {
  if (!token) return false;
  const redis = cache.raw();
  if (redis) return Boolean(await redis.get(ADMITTED_PREFIX + token));
  pruneMemory();
  return memAdmitted.has(token);
}

// Called once a user finishes booking, so their slot frees up.
async function release(token) {
  if (!token) return;
  const redis = cache.raw();
  if (redis) await redis.del(ADMITTED_PREFIX + token);
  else memAdmitted.delete(token);
}

async function stats() {
  const redis = cache.raw();
  if (redis) {
    return {
      enabled: isEnabled(),
      capacity: CAPACITY,
      waiting: await redis.zCard(WAITING_KEY),
      admitted: (await redis.keys(ADMITTED_PREFIX + '*')).length,
      backend: 'redis',
    };
  }
  pruneMemory();
  return {
    enabled: isEnabled(),
    capacity: CAPACITY,
    waiting: memWaiting.size,
    admitted: memAdmitted.size,
    backend: 'memory',
  };
}

// Express guard — only enforced when QUEUE_ENABLED=true.
async function requireQueueToken(req, res, next) {
  if (!isEnabled()) return next();
  const token = req.headers['x-queue-token'] || req.body?.queueToken;
  if (await isAdmitted(token)) return next();
  return res.status(429).json({
    error: 'You are in the waiting room. Join the queue and wait to be admitted.',
    queueRequired: true,
  });
}

module.exports = { isEnabled, join, getStatus, isAdmitted, release, stats, requireQueueToken };
