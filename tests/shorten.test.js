"use strict";

// Hoist mocks before any require() — Jest hoisting ensures these replace
// the real modules when src/index.js is loaded.
jest.mock("../src/db/postgres", () => ({
  query: jest.fn(),
  initSchema: jest.fn(),
}));

jest.mock("../src/cache/redis", () => ({
  get: jest.fn().mockResolvedValue(null),
  set: jest.fn().mockResolvedValue(undefined),
  del: jest.fn().mockResolvedValue(undefined),
  client: { on: jest.fn(), call: jest.fn() },
}));

// Replace the rate limiter with a pass-through — tests should not depend on Redis
// rate limit counters and redirects must never be blocked in the test suite.
jest.mock("../src/middleware/rateLimiter", () => (req, res, next) => next());

// Prevent Kafka connections from being opened during tests
jest.mock("../src/kafka/producer", () => ({
  emitClickEvent: jest.fn().mockResolvedValue(undefined),
}));

const request = require("supertest");
const app = require("../src/index");
const { query } = require("../src/db/postgres");
const cache = require("../src/cache/redis");

beforeEach(() => {
  jest.clearAllMocks();
  cache.get.mockResolvedValue(null); // default: cache miss
});

// ---------------------------------------------------------------------------
// POST /shorten
// ---------------------------------------------------------------------------

describe("POST /shorten", () => {
  test("201 with short_url for a valid URL", async () => {
    query.mockResolvedValueOnce({ rows: [] }); // INSERT succeeds

    const res = await request(app)
      .post("/shorten")
      .send({ url: "https://example.com/path" });

    expect(res.status).toBe(201);
    expect(res.body).toHaveProperty("short_url");
    expect(res.body).toHaveProperty("short_code");
    expect(res.body.expires_at).toBeNull();
  });

  test("201 and expires_at is set when ttl_days is provided", async () => {
    query.mockResolvedValueOnce({ rows: [] });

    const res = await request(app)
      .post("/shorten")
      .send({ url: "https://example.com", ttl_days: 7 });

    expect(res.status).toBe(201);
    expect(res.body.expires_at).not.toBeNull();
    const expiresAt = new Date(res.body.expires_at);
    expect(expiresAt.getTime()).toBeGreaterThan(Date.now());
  });

  test("400 for a missing URL", async () => {
    const res = await request(app).post("/shorten").send({});
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/invalid url/i);
  });

  test("400 for a non-http/https URL", async () => {
    const res = await request(app)
      .post("/shorten")
      .send({ url: "ftp://example.com" });
    expect(res.status).toBe(400);
  });

  test("400 for an alias that is too short (< 3 chars)", async () => {
    const res = await request(app)
      .post("/shorten")
      .send({ url: "https://example.com", alias: "ab" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/alias/i);
  });

  test("400 for an alias with special characters", async () => {
    const res = await request(app)
      .post("/shorten")
      .send({ url: "https://example.com", alias: "bad alias!" });
    expect(res.status).toBe(400);
  });

  test("409 when alias is already taken (PG unique violation)", async () => {
    const pgError = Object.assign(new Error("duplicate key"), {
      code: "23505",
    });
    query.mockRejectedValueOnce(pgError);

    const res = await request(app)
      .post("/shorten")
      .send({ url: "https://example.com", alias: "taken" });

    expect(res.status).toBe(409);
    expect(res.body.error).toMatch(/alias already taken/i);
  });
});

// ---------------------------------------------------------------------------
// GET /:code
// ---------------------------------------------------------------------------

describe("GET /:code", () => {
  test("301 redirect on cache miss — falls through to PostgreSQL", async () => {
    query.mockResolvedValueOnce({
      rows: [{ long_url: "https://example.com", expires_at: null }],
    });

    const res = await request(app).get("/abc123");

    expect(res.status).toBe(301);
    expect(res.headers.location).toBe("https://example.com");
  });

  test("301 redirect on cache hit — PostgreSQL is NOT queried", async () => {
    cache.get.mockResolvedValueOnce(
      JSON.stringify({ long_url: "https://cached.com", expires_at: null }),
    );

    const res = await request(app).get("/hit");

    expect(res.status).toBe(301);
    expect(res.headers.location).toBe("https://cached.com");
    expect(query).not.toHaveBeenCalled();
  });

  test("404 for an unknown code", async () => {
    query.mockResolvedValueOnce({ rows: [] });

    const res = await request(app).get("/unknown");
    expect(res.status).toBe(404);
  });

  test("410 Gone for an expired URL", async () => {
    const pastDate = new Date(Date.now() - 60_000).toISOString();
    query.mockResolvedValueOnce({
      rows: [{ long_url: "https://example.com", expires_at: pastDate }],
    });

    const res = await request(app).get("/expired");
    expect(res.status).toBe(410);
  });
});
