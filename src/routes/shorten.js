'use strict';

const express = require('express');
const { shortenUrl } = require('../services/shortener');

const router = express.Router();

const MAX_URL_LENGTH = 2048;
// Aliases that would collide with routes or look official.
const RESERVED_ALIASES = new Set(['shorten', 'stats', 'api', 'admin', 'health', 'static']);

// Returns the normalised URL (what we store), or null if invalid.
// typeof check first: new URL() and RegExp.test() coerce arrays to strings.
function parseUrl(str) {
  if (typeof str !== 'string' || str.length > MAX_URL_LENGTH) return null;
  try {
    const u = new URL(str);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    if (u.username || u.password) return null;
    return u.href;
  } catch {
    return null;
  }
}

// Validate custom alias: reserve generated-code space by capping aliases below 10 chars.
function isValidAlias(str) {
  return (
    typeof str === 'string' &&
    /^[a-zA-Z0-9-]{3,9}$/.test(str) &&
    !RESERVED_ALIASES.has(str.toLowerCase())
  );
}

router.post('/', async (req, res, next) => {
  try {
    const { url, alias, ttl_days: ttlDays } = req.body;

    const normalisedUrl = parseUrl(url);
    if (!normalisedUrl) {
      return res.status(400).json({ error: 'Invalid URL format' });
    }

    if (alias !== undefined && alias !== null && alias !== '' && !isValidAlias(alias)) {
      return res.status(400).json({
        error: 'Alias must be 3–9 characters: letters, numbers, or hyphens only',
      });
    }

    if (ttlDays !== undefined) {
      if (!Number.isInteger(ttlDays) || ttlDays < 1 || ttlDays > 3650) {
        return res.status(400).json({
          error: 'ttl_days must be an integer between 1 and 3650',
        });
      }
    }

    const { shortCode, expiresAt } = await shortenUrl({
      url: normalisedUrl,
      alias: alias || undefined,
      ttlDays,
    });
    const baseUrl = process.env.BASE_URL || 'http://localhost:3000';

    return res.status(201).json({
      short_url: `${baseUrl}/${shortCode}`,
      short_code: shortCode,
      expires_at: expiresAt ? expiresAt.toISOString() : null,
    });
  } catch (err) {
    // Only the short_code constraint maps to a user-facing alias collision.
    if (err.code === '23505' && err.constraint === 'urls_short_code_key') {
      return res.status(409).json({ error: 'Alias already taken' });
    }
    next(err);
  }
});

module.exports = router;
