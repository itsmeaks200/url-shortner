'use strict';

const Redis = require('ioredis');

const client = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  lazyConnect: true,        // don't connect until first command
  enableOfflineQueue: false, // fail fast if Redis is down — never block redirects
  maxRetriesPerRequest: 1,
});

client.on('error', (err) => {
  // Log but never crash — Redis is a cache, not the source of truth
  console.error('[Redis] Connection error:', err.message);
});

/**
 * Get a cached value. Returns null on miss or any Redis error.
 * @param {string} key
 * @returns {Promise<string|null>}
 */
async function get(key) {
  try {
    return await client.get(key);
  } catch (err) {
    console.error(`[Redis] GET failed for key "${key}":`, err.message);
    return null;
  }
}

/**
 * Set a key with an optional TTL in seconds.
 * Fails silently — a cache write failure must never break a redirect.
 * @param {string} key
 * @param {string} value
 * @param {number} [ttlSeconds]
 */
async function set(key, value, ttlSeconds) {
  try {
    if (ttlSeconds) {
      await client.set(key, value, 'EX', ttlSeconds);
    } else {
      await client.set(key, value);
    }
  } catch (err) {
    console.error(`[Redis] SET failed for key "${key}":`, err.message);
  }
}

/**
 * Delete a key (used for cache invalidation).
 * @param {string} key
 */
async function del(key) {
  try {
    await client.del(key);
  } catch (err) {
    console.error(`[Redis] DEL failed for key "${key}":`, err.message);
  }
}

module.exports = { client, get, set, del };
