'use strict';

const rateLimit = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const { client } = require('../cache/redis');

// Applied only to POST /shorten — redirects must never be throttled.
// Redis store is shared across all Express instances, so the counter is
// accurate even when running multiple processes behind a load balancer.
const store = new RedisStore({
  sendCommand: (...args) => client.call(...args),
  prefix: 'rl:',
});
// The store starts SCRIPT LOAD in its constructor; with Redis down at boot those
// promises reject unhandled and crash the process. The store reloads the script
// on its next increment, so swallowing these is safe.
store.incrementScriptSha.catch(() => {});
store.getScriptSha.catch(() => {});

module.exports = rateLimit({
  windowMs: 60 * 1000, // 1-minute fixed window
  max: 10,             // 10 shortens per IP per minute
  standardHeaders: true,
  legacyHeaders: false,
  // Fail open: if Redis is down, pass through rather than blocking requests
  passOnStoreError: true,
  store,
  message: { error: 'Too many requests. Try again in a minute.' },
});
