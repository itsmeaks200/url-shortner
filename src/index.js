"use strict";

require("dotenv").config();
const express = require("express");
const path = require("path");

const shortenRouter = require("./routes/shorten");
const redirectRouter = require("./routes/redirect");
const rateLimiter = require("./middleware/rateLimiter");
const errorHandler = require("./middleware/errorHandler");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "frontend")));

// Rate limiter applies only to POST /shorten — never throttle redirects
app.use("/shorten", rateLimiter, shortenRouter);
app.use("/", redirectRouter);

app.use(errorHandler);

// Guard prevents the server from starting when the module is required by tests
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`URL Shortener running on port ${PORT}`);
  });
}

module.exports = app;
