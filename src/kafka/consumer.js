'use strict';

const { Kafka } = require('kafkajs');
const { query } = require('../db/postgres');
const { sendToTopic } = require('./producer');

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
    // Manual offset commit: only advance the offset after the event is durably recorded.
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, heartbeat, isRunning, isStale }) => {
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          return;
        }

        try {
          const event = JSON.parse(message.value.toString());

          if (!event.event_id || !event.short_code || !event.timestamp) {
            throw new Error('Malformed click event');
          }

          const clickedAt = new Date(event.timestamp);
          if (Number.isNaN(clickedAt.getTime())) {
            throw new Error('Invalid click event timestamp');
          }

          const inserted = await query(
            `INSERT INTO click_events (event_id, short_code, clicked_at, ip_hash, user_agent, referrer)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (event_id) DO NOTHING
             RETURNING event_id`,
            [
              event.event_id,
              event.short_code,
              clickedAt,
              event.ip_hash || null,
              event.user_agent || null,
              event.referrer || null,
            ],
          );

          if (inserted.rowCount > 0) {
            const date = clickedAt.toISOString().slice(0, 10);

            await query(
              `INSERT INTO click_stats (short_code, date, click_count)
               VALUES ($1, $2, 1)
               ON CONFLICT (short_code, date)
               DO UPDATE SET click_count = click_stats.click_count + 1`,
              [event.short_code, date],
            );
          }

          resolveOffset(message.offset);
          await commitOffsetsIfNecessary();
        } catch (err) {
          if (
            err.message === 'Malformed click event' ||
            err.message === 'Invalid click event timestamp' ||
            err instanceof SyntaxError
          ) {
            console.error('[Kafka Consumer] Sending malformed event to DLQ:', err.message);
            try {
              await sendToTopic('click-events-dlq', {
                original_topic: batch.topic,
                partition: batch.partition,
                offset: message.offset,
                value: message.value ? message.value.toString() : '',
                error: err.message,
              });
              resolveOffset(message.offset);
              await commitOffsetsIfNecessary();
              continue;
            } catch (dlqErr) {
              console.error('[Kafka Consumer] Failed to write to DLQ:', dlqErr.message);
            }
          }

          console.error('[Kafka Consumer] Failed to process message:', err.message);
          throw err;
        }

        await heartbeat();
      }
    },
  });

  console.log('[Kafka Consumer] Listening on click-events topic');
}

async function stop() {
  await consumer.disconnect();
}

module.exports = { start, stop };
