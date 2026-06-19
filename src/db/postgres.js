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

async function initSchema() {
  await query(`
    CREATE TABLE IF NOT EXISTS urls (
      id BIGINT PRIMARY KEY,
      short_code VARCHAR(12) UNIQUE NOT NULL,
      long_url TEXT NOT NULL,
      custom_alias VARCHAR(50),
      created_at TIMESTAMP DEFAULT NOW(),
      expires_at TIMESTAMP
    );
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS click_stats (
      short_code VARCHAR(12) NOT NULL,
      date DATE NOT NULL,
      click_count INT DEFAULT 0,
      PRIMARY KEY (short_code, date)
    );
  `);

  console.log("Database schema initialised");
}

module.exports = { query, initSchema };
