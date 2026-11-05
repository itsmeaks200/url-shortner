'use strict';

let runConfig;
const mockCommitOffsets = jest.fn().mockResolvedValue(undefined);
const mockSendToTopic = jest.fn().mockResolvedValue(undefined);
const mockQuery = jest.fn();

jest.mock('kafkajs', () => ({
  Kafka: class {
    consumer() {
      return {
        connect: async () => {},
        subscribe: async () => {},
        run: async (cfg) => { runConfig = cfg; },
        commitOffsets: (...a) => mockCommitOffsets(...a),
      };
    }
  },
}));
jest.mock('../src/db/postgres', () => ({
  withTransaction: async (fn) => fn({ query: (...a) => mockQuery(...a) }),
}));
jest.mock('../src/kafka/producer', () => ({
  sendToTopic: (...a) => mockSendToTopic(...a),
}));

const consumer = require('../src/kafka/consumer');

const ID = '11111111-1111-4111-8111-111111111111';
const msg = (offset, obj) => ({ offset: String(offset), value: Buffer.from(JSON.stringify(obj)) });
const ev = (id = ID, code = 'abc') => ({ event_id: id, short_code: code, timestamp: '2026-11-05T10:00:00.000Z' });

function runBatch(messages) {
  return runConfig.eachBatch({
    batch: { topic: 'click-events', partition: 0, messages },
    resolveOffset: jest.fn(),
    heartbeat: async () => {},
    isRunning: () => true,
    isStale: () => false,
  });
}

beforeAll(async () => {
  jest.spyOn(console, 'error').mockImplementation(() => {});
  jest.spyOn(console, 'log').mockImplementation(() => {});
  await consumer.start();
});

beforeEach(() => {
  jest.clearAllMocks();
  mockQuery.mockImplementation(async (sql, params) =>
    /INTO click_events/.test(sql)
      ? { rows: [...new Set(params[0])].map((event_id) => ({ event_id })) }
      : { rows: [] });
});

test('disables auto-resolve so unprocessed offsets are never marked done', () => {
  expect(runConfig.autoCommit).toBe(false);
  expect(runConfig.eachBatchAutoResolve).toBe(false);
});

test('commits the NEXT offset explicitly after the transaction', async () => {
  await runBatch([msg(41, ev())]);
  expect(mockCommitOffsets).toHaveBeenCalledWith([
    { topic: 'click-events', partition: 0, offset: '42' },
  ]);
});

test('a duplicate event_id inside one batch is counted once', async () => {
  await runBatch([msg(1, ev()), msg(2, ev())]);
  const statsCall = mockQuery.mock.calls.find(([sql]) => /INTO click_stats/.test(sql));
  expect(statsCall[1][2]).toEqual([1]);
});

test('null-value messages go to the DLQ and the offset still advances', async () => {
  await runBatch([{ offset: '5', value: null }]);
  expect(mockSendToTopic).toHaveBeenCalledWith('click-events-dlq', expect.any(Object));
  expect(mockCommitOffsets).toHaveBeenCalledWith([
    { topic: 'click-events', partition: 0, offset: '6' },
  ]);
});

test.each([
  ['non-UUID event_id', ev('not-a-uuid')],
  ['over-long short_code', ev(ID, 'this-code-is-way-longer-than-twelve')],
])('%s is routed to the DLQ, not the database', async (_name, bad) => {
  await runBatch([msg(1, bad)]);
  expect(mockSendToTopic).toHaveBeenCalledTimes(1);
  expect(mockQuery).not.toHaveBeenCalled();
});
