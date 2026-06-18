'use strict';

// Snowflake ID layout (64-bit):
// [ 41 bits: ms since custom epoch | 10 bits: worker ID | 12 bits: sequence ]
//
// Epoch: 2024-01-01T00:00:00.000Z
// Worker ID: 0–1023 via WORKER_ID env var
// Sequence: 0–4095 per ms per worker → ~4M IDs/sec/worker

const EPOCH = 1704067200000n; // 2024-01-01
const WORKER_ID_BITS = 10n;
const SEQUENCE_BITS = 12n;
const MAX_SEQUENCE = (1n << SEQUENCE_BITS) - 1n; // 4095

let sequence = 0n;
let lastTimestamp = -1n;

function now() {
  return BigInt(Date.now()) - EPOCH;
}

function generateId() {
  const workerId = BigInt(process.env.WORKER_ID || 1) & ((1n << WORKER_ID_BITS) - 1n);

  let ts = now();

  if (ts === lastTimestamp) {
    sequence = (sequence + 1n) & MAX_SEQUENCE;
    // Sequence exhausted within this ms — wait for next ms
    if (sequence === 0n) {
      while (ts <= lastTimestamp) ts = now();
    }
  } else {
    sequence = 0n;
  }

  lastTimestamp = ts;

  return (ts << (WORKER_ID_BITS + SEQUENCE_BITS)) |
         (workerId << SEQUENCE_BITS) |
         sequence;
}

module.exports = { generateId };
