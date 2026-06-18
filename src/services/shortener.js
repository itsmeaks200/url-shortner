"use strict";

const { query } = require("../db/postgres");
const { encode } = require("../utils/base62");
const { generateId } = require("../utils/snowflake");
const cache = require("../cache/redis");

const CACHE_TTL_DEFAULT = 86400; // 24 hours in seconds

async function shortenUrl({ url, alias, ttlDays }) {
  const id = generateId();
  const shortCode = alias || encode(id);
  const expiresAt = ttlDays
    ? new Date(Date.now() + ttlDays * 86400 * 1000)
    : null;

  await query(
    `INSERT INTO urls (id, short_code, long_url, custom_alias, expires_at)
     VALUES ($1, $2, $3, $4, $5)`,
    [id.toString(), shortCode, url, alias || null, expiresAt],
  );

  return { shortCode, expiresAt };
}

async function resolveCode(code) {
  const cacheKey = `url:${code}`;

  // 1. Check Redis cache first
  const cached = await cache.get(cacheKey);
  if (cached) {
    return JSON.parse(cached);
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
