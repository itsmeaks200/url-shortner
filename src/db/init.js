'use strict';

// Run this script once to initialise the database schema.
// Usage: node src/db/init.js
require('dotenv').config();
const { initSchema } = require('./postgres');

initSchema()
  .then(() => {
    console.log('Schema ready.');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Schema init failed:', err.message);
    process.exit(1);
  });
