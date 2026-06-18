'use strict';

describe('snowflake.generateId', () => {
  let generateId;

  beforeEach(() => {
    process.env.WORKER_ID = '1';
    // Reset module state between tests so sequence/lastTimestamp start fresh
    jest.resetModules();
    ({ generateId } = require('../src/utils/snowflake'));
  });

  test('returns a BigInt', () => {
    expect(typeof generateId()).toBe('bigint');
  });

  test('generates 100 unique IDs consecutively', () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateId().toString()));
    expect(ids.size).toBe(100);
  });

  test('IDs are monotonically non-decreasing', () => {
    const ids = Array.from({ length: 50 }, () => generateId());
    for (let i = 1; i < ids.length; i++) {
      expect(ids[i]).toBeGreaterThanOrEqual(ids[i - 1]);
    }
  });

  test('encodes the worker ID in bits 12–21', () => {
    process.env.WORKER_ID = '7';
    jest.resetModules();
    const { generateId: gen } = require('../src/utils/snowflake');
    const id = gen();
    const workerId = (id >> 12n) & 0x3FFn; // mask 10 bits
    expect(workerId).toBe(7n);
  });

  test('timestamp portion is recent (within last 5 seconds)', () => {
    const EPOCH = 1704067200000n;
    const id = generateId();
    const ts = id >> 22n; // shift off worker + sequence bits
    const msSinceEpoch = ts;
    const nowMs = BigInt(Date.now()) - EPOCH;
    expect(msSinceEpoch).toBeGreaterThan(nowMs - 5000n);
    expect(msSinceEpoch).toBeLessThanOrEqual(nowMs + 100n);
  });
});
