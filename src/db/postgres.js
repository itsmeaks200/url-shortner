"use strict";

const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.POSTGRES_URL,
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
  await query(`
    CREATE TABLE IF NOT EXISTS urls (
      id BIGINT PRIMARY KEY,
      short_code VARCHAR(50) UNIQUE NOT NULL,
      long_url TEXT NOT NULL,
      custom_alias VARCHAR(50),
      created_at TIMESTAMP DEFAULT NOW(),
      expires_at TIMESTAMP
    );
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS click_stats (
      short_code VARCHAR(50) NOT NULL,
      date DATE NOT NULL,
      click_count INT DEFAULT 0,
      PRIMARY KEY (short_code, date)
    );
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS click_events (
      event_id UUID PRIMARY KEY,
      short_code VARCHAR(50) NOT NULL,
      clicked_at TIMESTAMP NOT NULL,
      ip_hash TEXT,
      user_agent TEXT,
      referrer TEXT
    );
  `);

  console.log("Database schema initialised");
}

module.exports = { query, withTransaction, initSchema };
