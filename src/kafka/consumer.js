'use strict';

const { Kafka } = require('kafkajs');
const { withTransaction } = require('../db/postgres');
const { sendToTopic } = require('./producer');

const kafka = new Kafka({
  clientId: 'url-shortener-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  logLevel: 1, // ERROR only
});

const consumer = kafka.consumer({ groupId: 'analytics-consumers' });

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

async function start() {
  await consumer.connect();

  // fromBeginning: false — on first start, only process new events.
  // On restart, picks up from the last committed offset automatically.
  await consumer.subscribe({ topic: 'click-events', fromBeginning: false });

  await consumer.run({
    // Manual offset commit: only advance the offset after the event is durably recorded.
    autoCommit: false,
    // Don't mark the batch resolved on return; we resolve only what we processed.
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      // 1. Parse/validate the whole batch; malformed events go to the DLQ.
      const events = [];
      for (const message of batch.messages) {
        if (!isRunning() || isStale()) {
          return;
        }

        try {
          // A null value (tombstone) becomes '' so JSON.parse throws SyntaxError -> DLQ.
          const event = JSON.parse(message.value ? message.value.toString() : '');

          // Check validity, not just presence: a bad UUID or over-long code would
          // fail the whole transaction and wedge the partition.
          if (
            !event ||
            typeof event.event_id !== 'string' || !UUID_RE.test(event.event_id) ||
            typeof event.short_code !== 'string' || event.short_code.length > 12 ||
            !event.timestamp
          ) {
            throw new Error('Malformed click event');
          }

          const clickedAt = new Date(event.timestamp);
          if (Number.isNaN(clickedAt.getTime())) {
            throw new Error('Invalid click event timestamp');
          }

          events.push({ ...event, clickedAt });
        } catch (err) {
          if (
            err.message !== 'Malformed click event' &&
            err.message !== 'Invalid click event timestamp' &&
            !(err instanceof SyntaxError)
          ) {
            console.error('[Kafka Consumer] Failed to process message:', err.message);
            throw err;
          }

          console.error('[Kafka Consumer] Sending malformed event to DLQ:', err.message);
          try {
            await sendToTopic('click-events-dlq', {
              original_topic: batch.topic,
              partition: batch.partition,
              offset: message.offset,
              value: message.value ? message.value.toString() : '',
              error: err.message,
            });
          } catch (dlqErr) {
            console.error('[Kafka Consumer] Failed to write to DLQ:', dlqErr.message);
            throw err;
          }
        }
      }

      // 2. Insert events and fold the newly-inserted ones into per-(short_code, date)
      //    counts, all in one transaction. Offsets are only resolved after COMMIT, so a
      //    crash redelivers the batch and ON CONFLICT (event_id) keeps it idempotent.
      if (events.length > 0) {
        await withTransaction(async (client) => {
          const inserted = await client.query(
            `INSERT INTO click_events (event_id, short_code, clicked_at, ip_hash, user_agent, referrer)
             SELECT * FROM unnest($1::uuid[], $2::varchar[], $3::timestamp[], $4::text[], $5::text[], $6::text[])
             ON CONFLICT (event_id) DO NOTHING
             RETURNING event_id`,
            [
              events.map((e) => e.event_id),
              events.map((e) => e.short_code),
              events.map((e) => e.clickedAt),
              events.map((e) => e.ip_hash || null),
              events.map((e) => e.user_agent || null),
              events.map((e) => e.referrer || null),
            ],
          );

          // delete() is true only the first time, so a duplicate inside one batch
          // is counted once.
          const fresh = new Set(inserted.rows.map((r) => r.event_id));
          const counts = new Map();
          for (const e of events) {
            if (!fresh.delete(e.event_id)) continue;
            const date = e.clickedAt.toISOString().slice(0, 10);
            const key = `${e.short_code}\u0000${date}`;
            const entry = counts.get(key);
            if (entry) {
              entry.count += 1;
            } else {
              counts.set(key, { short_code: e.short_code, date, count: 1 });
            }
          }

          if (counts.size > 0) {
            // Sorted so concurrent consumers lock rows in the same order (no deadlocks).
            const rows = [...counts.values()].sort(
              (a, b) => (a.short_code < b.short_code ? -1 : a.short_code > b.short_code ? 1 : a.date < b.date ? -1 : a.date > b.date ? 1 : 0),
            );
            await client.query(
              `INSERT INTO click_stats (short_code, date, click_count)
               SELECT * FROM unnest($1::varchar[], $2::date[], $3::int[])
               ON CONFLICT (short_code, date)
               DO UPDATE SET click_count = click_stats.click_count + EXCLUDED.click_count`,
              [rows.map((r) => r.short_code), rows.map((r) => r.date), rows.map((r) => r.count)],
            );
          }
        });
      }

      // 3. One offset resolve + commit for the whole batch.
      if (batch.messages.length > 0) {
        // commitOffsetsIfNecessary() is a no-op without autoCommitInterval/Threshold,
        // so commit explicitly. Kafka stores the NEXT offset to read.
        const last = batch.messages[batch.messages.length - 1].offset;
        resolveOffset(last);
        await consumer.commitOffsets([
          {
            topic: batch.topic,
            partition: batch.partition,
            offset: (BigInt(last) + 1n).toString(),
          },
        ]);
      }
      await heartbeat();
    },
  });

  console.log('[Kafka Consumer] Listening on click-events topic');
}

async function stop() {
  await consumer.disconnect();
}

module.exports = { start, stop };
