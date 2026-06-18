'use strict';

const express = require('express');
const { resolveCode } = require('../services/shortener');

const router = express.Router();

router.get('/:code', async (req, res, next) => {
  try {
    const { code } = req.params;
    const record = await resolveCode(code);

    if (!record) {
      return res.status(404).json({ error: 'Short URL not found' });
    }

    if (record.expires_at && new Date() > new Date(record.expires_at)) {
      return res.status(410).json({ error: 'This short URL has expired' });
    }

    return res.redirect(301, record.long_url);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
