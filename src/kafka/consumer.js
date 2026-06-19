'use strict';

const { Kafka } = require('kafkajs');
const { query } = require('../db/postgres');

const kafka = new Kafka({
  clientId: 'url-shortener-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: 1, // ERROR only
});

const consumer = kafka.consumer({ groupId: 'analytics-consumers' });

async function start() {
  await consumer.connect();

  // fromBeginning: false — on first start, only process new events.
  // On restart, picks up from the last committed offset automatically.
  await consumer.subscribe({ topic: 'click-events', fromBeginning: false });

  await consumer.run({
    // Manual offset commit: only advance the offset after the DB write succeeds.
    // If the consumer crashes between the write and the commit, the message is
    // re-processed. The upsert is idempotent so this at-least-once behaviour is safe.
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const event = JSON.parse(message.value.toString());
        const date = event.timestamp.split('T')[0]; // YYYY-MM-DD

        await query(
          `INSERT INTO click_stats (short_code, date, click_count)
           VALUES ($1, $2, 1)
           ON CONFLICT (short_code, date)
           DO UPDATE SET click_count = click_stats.click_count + 1`,
          [event.short_code, date],
        );

        // Commit next offset only after the DB write succeeds
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } catch (err) {
        // Log and skip — do NOT commit the offset so the message is retried on restart
        console.error('[Kafka Consumer] Failed to process message:', err.message);
      }
    },
  });

  console.log('[Kafka Consumer] Listening on click-events topic');
}

async function stop() {
  await consumer.disconnect();
}

module.exports = { start, stop };
