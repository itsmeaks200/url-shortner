'use strict';

const { query } = require('../db/postgres');
const { encode } = require('../utils/base62');

// Temporary ID generator for Phase 1 — replaced by Snowflake in Phase 2
function tempId() {
  // Use current timestamp + random suffix to produce a unique BIGINT
  const ts = BigInt(Date.now());
  const rand = BigInt(Math.floor(Math.random() * 100000));
  return ts * 100000n + rand;
}

async function shortenUrl({ url, alias, ttlDays }) {
  const id = tempId();
  const shortCode = alias || encode(id);
  const expiresAt = ttlDays
    ? new Date(Date.now() + ttlDays * 86400 * 1000)
    : null;

  await query(
    `INSERT INTO urls (id, short_code, long_url, custom_alias, expires_at)
     VALUES ($1, $2, $3, $4, $5)`,
    [id.toString(), shortCode, url, alias || null, expiresAt]
  );

  return { shortCode, expiresAt };
}

async function resolveCode(code) {
  const result = await query(
    `SELECT long_url, expires_at FROM urls WHERE short_code = $1`,
    [code]
  );
  return result.rows[0] || null;
}

module.exports = { shortenUrl, resolveCode };
