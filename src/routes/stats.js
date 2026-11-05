'use strict';

const express = require('express');
const { getStats } = require('../services/analytics');

const router = express.Router();

router.get('/:code/stats', async (req, res, next) => {
  try {
    if (!/^[a-zA-Z0-9-]{1,12}$/.test(req.params.code)) {
      return res.status(404).json({ error: 'Short code not found' });
    }
    const stats = await getStats(req.params.code);

    if (!stats) {
      return res.status(404).json({ error: 'Short code not found' });
    }

    return res.json(stats);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
