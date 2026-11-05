"use strict";

const { query } = require("../db/postgres");
const { encode } = require("../utils/base62");
const { generateId } = require("../utils/snowflake");
const cache = require("../cache/redis");

const CACHE_TTL_DEFAULT = 86400; // 24 hours in seconds
const MAX_GENERATION_ATTEMPTS = 3;

async function shortenUrl({ url, alias, ttlDays }) {
  const expiresAt = ttlDays
    ? new Date(Date.now() + ttlDays * 86400 * 1000)
    : null;

  for (let attempt = 0; attempt < MAX_GENERATION_ATTEMPTS; attempt++) {
    const id = generateId();
    const shortCode = alias || encode(id);

    try {
      await query(
        `INSERT INTO urls (id, short_code, long_url, expires_at)
         VALUES ($1, $2, $3, $4)`,
        [id.toString(), shortCode, url, expiresAt],
      );

      return { shortCode, expiresAt };
    } catch (err) {
      const isUniqueViolation = err.code === '23505';
      const isShortCodeCollision = isUniqueViolation && err.constraint === 'urls_short_code_key';
      const isGeneratedIdCollision = isUniqueViolation && err.constraint === 'urls_pkey';

      if (alias && isShortCodeCollision) {
        throw err;
      }

      if ((isGeneratedIdCollision || (!alias && isShortCodeCollision)) && attempt < MAX_GENERATION_ATTEMPTS - 1) {
        continue;
      }

      if (isGeneratedIdCollision || (!alias && isShortCodeCollision)) {
        const error = new Error('Failed to allocate a unique short code');
        error.code = 'EGENERATEID';
        error.status = 503;
        throw error;
      }

      throw err;
    }
  }

  const error = new Error('Failed to allocate a unique short code');
  error.code = 'EGENERATEID';
  error.status = 503;
  throw error;
}

async function resolveCode(code) {
  const cacheKey = `url:${code}`;

  // 1. Check Redis cache first
  const cached = await cache.get(cacheKey);
  if (cached) {
    try {
      return JSON.parse(cached);
    } catch {
      // Corrupt entry: treat as a miss and drop it so it can't 500 until TTL.
      console.error(`[Cache] Corrupt value for ${cacheKey}; evicting`);
      await cache.del(cacheKey);
    }
  }

  // 2. Cache miss — query PostgreSQL
  const result = await query(
    `SELECT long_url, expires_at FROM urls WHERE short_code = $1`,
    [code],
  );
  const row = result.rows[0] || null;

  // 3. Write to cache on hit (don't cache misses)
  if (row) {
    // For expiring URLs use min(24h, time_until_expiry) as TTL
    let ttl = CACHE_TTL_DEFAULT;
    if (row.expires_at) {
      const secondsLeft = Math.floor(
        (new Date(row.expires_at) - Date.now()) / 1000,
      );
      if (secondsLeft <= 0) {
        // Already expired — don't cache, let redirect handler return 410
        return row;
      }
      ttl = Math.min(CACHE_TTL_DEFAULT, secondsLeft);
    }
    await cache.set(cacheKey, JSON.stringify(row), ttl);
  }

  return row;
}

module.exports = { shortenUrl, resolveCode };
