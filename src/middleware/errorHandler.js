'use strict';

// eslint-disable-next-line no-unused-vars
function errorHandler(err, req, res, next) {
  // body-parser and the service set err.status / err.statusCode (400, 413, 503...).
  const status = err.status || err.statusCode || 500;
  if (status >= 500) console.error(`[Error] ${err.message}`);
  res.status(status).json({
    error: status < 500 ? 'Bad request' : status === 503 ? 'Service unavailable' : 'Internal server error',
  });
}

module.exports = errorHandler;
