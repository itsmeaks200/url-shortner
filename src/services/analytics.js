'use strict';

const { query } = require('../db/postgres');

/**
 * Return click statistics for a short code.
 * Returns null if the code does not exist in the urls table.
 * @param {string} code
 */
async function getStats(code) {
  // Confirm the short code exists
  const urlResult = await query(
    `SELECT long_url FROM urls WHERE short_code = $1`,
    [code],
  );

  if (!urlResult.rows[0]) return null;

  const { long_url } = urlResult.rows[0];

  // All-time total — do not limit to 30 days here
  const totalResult = await query(
    `SELECT COALESCE(SUM(click_count), 0)::int AS total
     FROM click_stats
     WHERE short_code = $1`,
    [code],
  );

  // Per-day breakdown for the last 30 days, newest first
  const daysResult = await query(
    `SELECT date::TEXT, click_count
     FROM click_stats
     WHERE short_code = $1
     ORDER BY date DESC
     LIMIT 30`,
    [code],
  );

  return {
    short_code: code,
    long_url,
    total_clicks: totalResult.rows[0].total,
    clicks_by_day: daysResult.rows.map((r) => ({
      date: r.date,
      count: parseInt(r.click_count, 10),
    })),
  };
}

module.exports = { getStats };
