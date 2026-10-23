// src/config/cache.js
//
// A tiny cache facade. Uses Redis when REDIS_URL is set AND the `redis`
// package is installed; otherwise falls back to an in-process Map.
//
// The fallback matters for deployment: hosts without a Redis add-on still
// run the app unchanged, just without cross-instance cache sharing.

const logger = require('./logger');

let redisClient = null;
let mode = 'memory';

const memory = new Map(); // key -> { value, expiresAt }

async function init() {
  if (!process.env.REDIS_URL) {
    logger.info({ cache: 'memory' }, 'no REDIS_URL — using in-process cache');
    return;
  }
  try {
    const { createClient } = require('redis');
    redisClient = createClient({ url: process.env.REDIS_URL });
    redisClient.on('error', (err) => logger.warn({ err: err.message }, 'redis error'));
    await redisClient.connect();
    mode = 'redis';
    logger.info({ cache: 'redis', url: process.env.REDIS_URL }, 'connected to Redis');
  } catch (err) {
    redisClient = null;
    mode = 'memory';
    logger.warn({ err: err.message }, 'Redis unavailable — falling back to in-process cache');
  }
}

async function get(key) {
  if (mode === 'redis') {
    const raw = await redisClient.get(key);
    return raw ? JSON.parse(raw) : null;
  }
  const hit = memory.get(key);
  if (!hit) return null;
  if (hit.expiresAt < Date.now()) { memory.delete(key); return null; }
  return hit.value;
}

async function set(key, value, ttlSeconds = 5) {
  if (mode === 'redis') {
    await redisClient.set(key, JSON.stringify(value), { EX: ttlSeconds });
    return;
  }
  memory.set(key, { value, expiresAt: Date.now() + ttlSeconds * 1000 });
}

// Invalidate every key under a prefix (called after any write).
async function invalidate(prefix) {
  if (mode === 'redis') {
    const keys = await redisClient.keys(`${prefix}*`);
    if (keys.length) await redisClient.del(keys);
    return;
  }
  for (const k of memory.keys()) if (k.startsWith(prefix)) memory.delete(k);
}

// Raw client for the queue (sorted sets); null in memory mode.
function raw() { return redisClient; }
function getMode() { return mode; }

module.exports = { init, get, set, invalidate, raw, getMode };
