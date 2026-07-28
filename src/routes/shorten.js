'use strict';

const express = require('express');
const { shortenUrl } = require('../services/shortener');

const router = express.Router();

// Validate URL format
function isValidUrl(str) {
  try {
    const u = new URL(str);
    return u.protocol === 'http:' || u.protocol === 'https:';
  } catch {
    return false;
  }
}

// Validate custom alias: reserve generated-code space by capping aliases below 10 chars.
function isValidAlias(str) {
  return /^[a-zA-Z0-9-]{3,9}$/.test(str);
}

router.post('/', async (req, res, next) => {
  try {
    const { url, alias, ttl_days: ttlDays } = req.body;

    if (!url || !isValidUrl(url)) {
      return res.status(400).json({ error: 'Invalid URL format' });
    }

    if (alias && !isValidAlias(alias)) {
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

    const { shortCode, expiresAt } = await shortenUrl({ url, alias, ttlDays });
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
