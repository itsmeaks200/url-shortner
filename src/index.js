"use strict";

require("dotenv").config();
const express = require("express");
const path = require("path");

const shortenRouter = require("./routes/shorten");
const redirectRouter = require("./routes/redirect");
const statsRouter = require("./routes/stats");
const rateLimiter = require("./middleware/rateLimiter");
const errorHandler = require("./middleware/errorHandler");

const app = express();
const PORT = process.env.PORT || 3000;

// Behind nginx: trust one proxy hop so req.ip is the client, not the proxy.
// Needed for per-IP rate limiting and click IP hashing.
app.set("trust proxy", 1);

// Limit before parsing so rejected requests cost as little as possible.
// POST only: other methods on /shorten must not consume a slot. Never throttle redirects.
app.post("/shorten", rateLimiter);

app.use(express.json());

// The frontend is one file; a dedicated route avoids an fs.stat on every redirect
// that express.static would do.
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "index.html"));
});

app.use("/shorten", shortenRouter);
// /:code matches a single path segment, so mount order of these two is not significant
app.use("/", statsRouter);
app.use("/", redirectRouter);

app.use(errorHandler);

// Guard prevents the server from starting when the module is required by tests
if (require.main === module) {
  const { initSchema } = require("./db/postgres");

  // Initialise DB schema before accepting traffic, then start everything else.
  // initSchema uses IF NOT EXISTS so it is safe to run on every boot.
  initSchema()
    .then(() => {
      const server = app.listen(PORT, () => {
        console.log(`URL Shortener running on port ${PORT}`);
      });

      // Start the Kafka consumer in the background, retrying with backoff + jitter:
      // Kafka often isn't ready when the app boots, and an outage must not stop HTTP.
      const consumer = require("./kafka/consumer");
      let stopping = false;
      (async () => {
        for (let attempt = 0; !stopping; attempt++) {
          try {
            await consumer.start();
            return;
          } catch (err) {
            const delay = Math.random() * Math.min(30000, 1000 * 2 ** attempt);
            console.error(`[Kafka] Consumer failed to start (retry in ${Math.round(delay)}ms):`, err.message);
            await new Promise((r) => setTimeout(r, delay));
          }
        }
      })();

      // Graceful shutdown: stop taking requests, leave the consumer group cleanly so
      // partitions move immediately, flush the producer, then close Redis.
      const shutdown = async () => {
        stopping = true;
        const force = setTimeout(() => process.exit(1), 8000);
        force.unref();
        server.close();
        await Promise.allSettled([
          consumer.stop(),
          require("./kafka/producer").disconnect(),
          require("./cache/redis").client.quit(),
        ]);
        process.exit(0);
      };
      process.on("SIGTERM", shutdown);
      process.on("SIGINT", shutdown);
    })
    .catch((err) => {
      console.error("[Startup] Schema initialisation failed:", err.message);
      process.exit(1);
    });
}

module.exports = app;
