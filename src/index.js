'use strict';

require('dotenv').config();
const express = require('express');
const path = require('path');

const shortenRouter = require('./routes/shorten');
const redirectRouter = require('./routes/redirect');
const errorHandler = require('./middleware/errorHandler');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'frontend')));

app.use('/shorten', shortenRouter);
app.use('/', redirectRouter);

app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`URL Shortener running on port ${PORT}`);
});

module.exports = app;
