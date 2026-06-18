'use strict';

// eslint-disable-next-line no-unused-vars
function errorHandler(err, req, res, next) {
  console.error(`[Error] ${err.message}`);
  res.status(500).json({ error: 'Internal server error' });
}

module.exports = errorHandler;
