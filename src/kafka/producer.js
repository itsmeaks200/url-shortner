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

async function sendToTopic(topic, payload) {
  await connect();

  const value = typeof payload === 'string' ? payload : JSON.stringify(payload);

  await producer.send({
    topic,
    messages: [{ value }],
  });
}

/**
 * Emit a click event to the `click-events` topic.
 * Raw IPs are never stored — only a SHA-256 hash.
 * Called fire-and-forget from the redirect handler.
 * @param {string} shortCode
 * @param {import('express').Request} req
 */
async function emitClickEvent(shortCode, req) {
  const rawIp = req.ip || req.socket.remoteAddress || 'unknown';
  const ipHash = crypto.createHash('sha256').update(rawIp).digest('hex');

  const event = {
    event_id: crypto.randomUUID(),
    short_code: shortCode,
    timestamp: new Date().toISOString(),
    ip_hash: ipHash,
    user_agent: req.headers['user-agent'] || '',
    // referrer: req.headers.referer || req.headers.referrer || '', // Removed for privacy
  };

  await sendToTopic('click-events', event);
}

async function disconnect() {
  if (connected) {
    await producer.disconnect();
    connected = false;
  }
}

module.exports = { emitClickEvent, disconnect, sendToTopic };
