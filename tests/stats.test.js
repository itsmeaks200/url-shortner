'use strict';

jest.mock('../src/db/postgres', () => ({
  query: jest.fn(),
  initSchema: jest.fn(),
}));

jest.mock('../src/cache/redis', () => ({
  get: jest.fn().mockResolvedValue(null),
  set: jest.fn().mockResolvedValue(undefined),
  del: jest.fn().mockResolvedValue(undefined),
  client: { on: jest.fn(), call: jest.fn() },
}));

jest.mock('../src/middleware/rateLimiter', () => (req, res, next) => next());
jest.mock('../src/kafka/producer', () => ({
  emitClickEvent: jest.fn().mockResolvedValue(undefined),
}));

const request = require('supertest');
const app = require('../src/index');
const { query } = require('../src/db/postgres');

beforeEach(() => {
  jest.clearAllMocks();
});

describe('GET /:code/stats', () => {
  test('200 with full stats for a known code with clicks', async () => {
    // 1st query: urls table lookup
    query.mockResolvedValueOnce({
      rows: [{ long_url: 'https://example.com/original' }],
    });
    // 2nd query: total clicks
    query.mockResolvedValueOnce({ rows: [{ total: 42 }] });
    // 3rd query: clicks by day
    query.mockResolvedValueOnce({
      rows: [
        { date: '2025-06-18', click_count: '30' },
        { date: '2025-06-17', click_count: '12' },
      ],
    });

    const res = await request(app).get('/abc123/stats');

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({
      short_code: 'abc123',
      long_url: 'https://example.com/original',
      total_clicks: 42,
      clicks_by_day: [
        { date: '2025-06-18', count: 30 },
        { date: '2025-06-17', count: 12 },
      ],
    });
  });

  test('200 with zero clicks when the URL has never been visited', async () => {
    query.mockResolvedValueOnce({
      rows: [{ long_url: 'https://example.com' }],
    });
    query.mockResolvedValueOnce({ rows: [{ total: 0 }] });
    query.mockResolvedValueOnce({ rows: [] });

    const res = await request(app).get('/newlink/stats');

    expect(res.status).toBe(200);
    expect(res.body.total_clicks).toBe(0);
    expect(res.body.clicks_by_day).toEqual([]);
  });

  test('404 for an unknown short code', async () => {
    query.mockResolvedValueOnce({ rows: [] }); // url not found

    const res = await request(app).get('/ghost/stats');

    expect(res.status).toBe(404);
    expect(res.body.error).toMatch(/not found/i);
  });

  test('stats route does not interfere with /:code redirect', async () => {
    // GET /abc123 should still redirect, not match /:code/stats
    query.mockResolvedValueOnce({
      rows: [{ long_url: 'https://example.com', expires_at: null }],
    });

    const res = await request(app).get('/abc123');
    expect(res.status).toBe(301);
    expect(res.headers.location).toBe('https://example.com');
  });
});
