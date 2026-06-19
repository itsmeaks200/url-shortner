'use strict';

const { Kafka } = require('kafkajs');
const crypto = require('crypto');

const kafka = new Kafka({
  clientId: 'url-shortener',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  // Suppress the default KafkaJS banner in logs
  logLevel: 1, // ERROR only
});

const producer = kafka.producer({ idempotent: true });
let connected = false;

async function connect() {
  if (!connected) {
    await producer.connect();
    connected = true;
  }
}

/**
 * Emit a click event to the `click-events` topic.
 * Raw IPs are never stored — only a SHA-256 hash.
 * Called fire-and-forget from the redirect handler.
 * @param {string} shortCode
 * @param {import('express').Request} req
 */
async function emitClickEvent(shortCode, req) {
  await connect();

  const rawIp = req.ip || req.socket.remoteAddress || 'unknown';
  const ipHash = crypto.createHash('sha256').update(rawIp).digest('hex');

  const event = {
    short_code: shortCode,
    timestamp: new Date().toISOString(),
    ip_hash: ipHash,
    user_agent: req.headers['user-agent'] || '',
    referrer: req.headers.referer || req.headers.referrer || '',
  };

  await producer.send({
    topic: 'click-events',
    messages: [{ value: JSON.stringify(event) }],
  });
}

async function disconnect() {
  if (connected) {
    await producer.disconnect();
    connected = false;
  }
}

module.exports = { emitClickEvent, disconnect };
