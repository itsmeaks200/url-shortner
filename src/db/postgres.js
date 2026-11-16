"use strict";

const { Pool } = require("pg");

// Budget: (app instances x app PG_POOL_MAX) + (consumers x consumer PG_POOL_MAX)
// must stay under Postgres max_connections (default 100), leaving some headroom.
const pool = new Pool({
  connectionString: process.env.POSTGRES_URL,
  max: parseInt(process.env.PG_POOL_MAX, 10) || 10,
  // Fail instead of waiting forever when the pool is exhausted; a hung acquire in the
  // consumer would stall heartbeats and trigger a rebalance.
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 30000,
});

pool.on("error", (err) => {
  console.error("Unexpected PostgreSQL client error:", err.message);
});

async function query(text, params) {
  const client = await pool.connect();
  try {
    const result = await client.query(text, params);
    return result;
  } finally {
    client.release();
  }
}

// Runs fn(client) inside BEGIN/COMMIT on a single pooled client; rolls back on error.
async function withTransaction(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {
      // ignore rollback failure; original error is more useful
    }
    throw err;
  } finally {
    client.release();
  }
}

async function initSchema() {
  // Advisory lock prevents two app instances from running CREATE TABLE
  // concurrently at startup, which would cause a unique constraint collision
  // on pg_type. Lock is automatically released when the session ends.
  await withTransaction(async (client) => {
    await client.query(`SELECT pg_advisory_xact_lock(1)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS urls (
        id BIGINT PRIMARY KEY,
        short_code VARCHAR(12) UNIQUE NOT NULL,
        long_url TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        expires_at TIMESTAMP
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS click_stats (
        short_code VARCHAR(12) NOT NULL,
        date DATE NOT NULL,
        click_count INT DEFAULT 0,
        PRIMARY KEY (short_code, date)
      );
    `);

    await client.query(`
      CREATE TABLE IF NOT EXISTS click_events (
        event_id UUID PRIMARY KEY,
        short_code VARCHAR(12) NOT NULL,
        clicked_at TIMESTAMP NOT NULL,
        ip_hash CHAR(64),
        user_agent TEXT,
        referrer TEXT
      );
    `);
  });

  console.log("Database schema initialised");
}

async function closePool() {
  await pool.end();
}

module.exports = { query, withTransaction, initSchema, closePool };
