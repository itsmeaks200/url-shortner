"use strict";

// Standalone Kafka consumer process. Runs the click-analytics consumer without the
// HTTP server so it can be scaled independently of the app (up to the number of
// partitions on the click-events topic).

require("dotenv").config();

const { initSchema, closePool } = require("./db/postgres");
const consumer = require("./kafka/consumer");
const producer = require("./kafka/producer");

let stopping = false;

async function startConsumer() {
  // Retry with backoff + jitter: Kafka often isn't ready at boot, and a transient
  // outage must not leave the worker permanently idle.
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
}

// Leave the consumer group cleanly so partitions are reassigned immediately, then
// flush the (DLQ) producer and close the DB pool.
async function shutdown() {
  if (stopping) return;
  stopping = true;
  const force = setTimeout(() => process.exit(1), 8000);
  force.unref();
  await Promise.allSettled([consumer.stop(), producer.disconnect()]);
  await Promise.allSettled([closePool()]);
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// initSchema is idempotent and serialised by an advisory lock, so it is safe to run
// from every worker and app instance on boot.
initSchema()
  .then(startConsumer)
  .catch((err) => {
    console.error("[Startup] Worker failed:", err.message);
    process.exit(1);
  });
