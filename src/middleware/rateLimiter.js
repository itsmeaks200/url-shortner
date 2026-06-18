'use strict';

const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const { client } = require('../cache/redis');

// Applied only to POST /shorten — redirects must never be throttled.
// Redis store is shared across all Express instances, so the counter is
// accurate even when running multiple processes behind a load balancer.
module.exports = rateLimit({
  windowMs: 60 * 1000, // 1-minute fixed window
  max: 10,             // 10 shortens per IP per minute
  standardHeaders: true,
  legacyHeaders: false,
  // Fail open: if Redis is down, pass through rather than blocking requests
  passOnStoreError: true,
  store: new RedisStore({
    sendCommand: (...args) => client.call(...args),
    prefix: 'rl:',
  }),
  message: { error: 'Too many requests. Try again in a minute.' },
});
