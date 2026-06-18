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

// Validate custom alias: 3–50 chars, alphanumeric + hyphens
function isValidAlias(str) {
  return /^[a-zA-Z0-9-]{3,50}$/.test(str);
}

router.post('/', async (req, res, next) => {
  try {
    const { url, alias, ttl_days: ttlDays } = req.body;

    if (!url || !isValidUrl(url)) {
      return res.status(400).json({ error: 'Invalid URL format' });
    }

    if (alias && !isValidAlias(alias)) {
      return res.status(400).json({
        error: 'Alias must be 3–50 characters: letters, numbers, or hyphens only',
      });
    }

    const { shortCode, expiresAt } = await shortenUrl({ url, alias, ttlDays });
    const baseUrl = process.env.BASE_URL || 'http://localhost:3000';

    return res.status(201).json({
      short_url: `${baseUrl}/${shortCode}`,
      short_code: shortCode,
      expires_at: expiresAt ? expiresAt.toISOString() : null,
    });
  } catch (err) {
    // Unique constraint violation — alias already taken
    if (err.code === '23505') {
      return res.status(409).json({ error: 'Alias already taken' });
    }
    next(err);
  }
});

module.exports = router;
