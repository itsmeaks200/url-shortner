# Distributed URL Shortener

A production-grade URL shortener built with Node.js, Express, PostgreSQL, Redis, and Kafka. 

## Architecture

```mermaid
flowchart TD
    Client([Browser / Client])

    subgraph API["Express API"]
        RL[Rate Limiter]
        SH[POST /shorten]
        RD[GET /:code]
        ST[GET /:code/stats]
    end

    subgraph Cache["Redis"]
        RC[(url:{code})]
    end

    subgraph DB["PostgreSQL"]
        UT[(urls)]
        CS[(click_stats)]
    end

    subgraph Pipeline["Kafka"]
        KP[Producer]
        KT[[click-events]]
        KC[Consumer]
    end

    Client -->|POST /shorten| RL --> SH
    Client -->|GET /:code| RD
    Client -->|GET /:code/stats| ST

    SH -->|Snowflake ID + Base62| UT
    RD -->|1. cache lookup| RC
    RC -->|miss| RD
    RD -->|2. DB fallback| UT
    RD -->|3. cache fill| RC
    RD -.->|fire & forget| KP
    KP --> KT --> KC -->|upsert| CS
    ST --> UT & CS
```

## Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| API | Node.js + Express | Fast, non-blocking I/O |
| Database | PostgreSQL 15 | Durable, ACID, indexed |
| Cache | Redis 7 | Sub-millisecond redirects |
| ID generation | Snowflake (custom) | Time-ordered, no coordination |
| URL encoding | Base62 | URL-safe, short, human-readable |
| Analytics | Kafka + KafkaJS | Decouples analytics from redirect critical path |
| Container | Docker Compose | Zero-cost local dev, one command setup |

## Running locally

**Prerequisites:** Docker and Docker Compose.

```bash
git clone <repo-url>
cd url-shortener
docker compose up --build
```

The app is available at `http://localhost:3000`. All services (Postgres, Redis, Zookeeper, Kafka) start automatically. The database schema is created on first boot.

> **Note:** Kafka takes ~20–30 seconds to become ready after `docker compose up`. The server starts and handles HTTP traffic immediately; the analytics consumer reconnects automatically once Kafka is ready.

### Run tests (no Docker required)

```bash
npm install
npm test
```

35 tests across 4 suites. All infrastructure (Postgres, Redis, Kafka) is mocked — tests run in ~1–4 seconds.

## API Reference

### `POST /shorten`

Shorten a URL, with an optional custom alias and TTL.

**Rate limit:** 10 requests per IP per minute.

```
POST /shorten
Content-Type: application/json
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `url` | string | ✅ | Must be `http://` or `https://` |
| `alias` | string | ❌ | 3–9 chars, alphanumeric + hyphens only |
| `ttl_days` | number | ❌ | Expiry in days from now (1–3650) |

**201 Created**
```json
{
  "short_url": "http://localhost:3000/abc123",
  "short_code": "abc123",
  "expires_at": "2025-07-01T00:00:00.000Z"
}
```

**Error responses**

| Status | Condition |
|--------|-----------|
| `400` | Invalid URL or alias format |
| `409` | Custom alias already taken |
| `429` | Rate limit exceeded |

---

### `GET /:code`

Redirect to the original URL.

| Status | Condition |
|--------|-----------|
| `307` | Temporary redirect to `long_url` (cached up to URL TTL) |
| `404` | Code not found |
| `410` | URL has expired |

**Caching:** Uses `Cache-Control: public, max-age={ttl}` where TTL equals the URL's remaining lifetime (or 1 hour default). This means URLs are cached as long as they're valid, maximizing performance while guaranteeing freshness.

---

### `GET /:code/stats`

Retrieve click analytics for a short URL.

**200 OK**
```json
{
  "short_code": "abc123",
  "long_url": "https://example.com/very/long/path",
  "total_clicks": 1042,
  "clicks_by_day": [
    { "date": "2025-06-18", "count": 312 },
    { "date": "2025-06-17", "count": 420 }
  ]
}
```

| Status | Condition |
|--------|-----------|
| `200` | Stats returned (zero clicks is valid) |
| `404` | Code not found |

---

## Design decisions

### Why Snowflake IDs instead of UUID or auto-increment?

UUIDs are random — every `INSERT` lands at a random position in the B-tree index, causing page splits and cache misses at scale. Auto-increment requires a sequence lock that doesn't work across multiple processes without coordination.

Snowflake IDs are 64-bit integers composed of: `| 41-bit timestamp | 10-bit worker ID | 12-bit sequence |`. They are monotonically increasing, so new rows always append to the end of the index. Worker ID (set via `WORKER_ID` env var) ensures uniqueness across instances without coordination. 4,096 IDs/ms/worker ≈ 4 million IDs/sec/worker.

### Why Base62 and not UUID strings as short codes?

Base62 (`0-9a-zA-Z`) avoids URL-unsafe characters (`+`, `/`, `=` from Base64). 7 characters of Base62 = 62⁷ ≈ 3.5 trillion unique codes. Short, human-readable, and typeable. The code is derived directly from the Snowflake ID via `encode(id)`, so no extra DB lookup is needed to confirm uniqueness.

### Why 307 with full TTL caching and not 301?

`307` (Temporary Redirect) with `Cache-Control: public, max-age={remaining_ttl}` provides optimal performance:
- **Full TTL caching:** If URL valid for 30 days, browser caches for 30 days (not just 5 minutes!)
- **Analytics:** Still tracks first click per user (which is the most valuable metric)
- **TTL respect:** Automatically sets cache to URL expiry time, never caches stale URLs
- **Configurable:** Set `BROWSER_CACHE_TTL=3600` (1 hour default) for URLs without expiry
- **Performance:** Viral link with 7-day TTL = 1 server hit per user (not millions)

**Alternatives:**
- `301` (Permanent): Cached forever, even after URL expires or destination changes
- `302` (no cache): Every click hits server - can't handle viral traffic

**Key insight:** "Unique visitors" (first click per user) is more valuable than "total page views" (includes refreshes, bots). Full TTL caching gives you the metrics that matter at web scale.

See [docs/CACHING_STRATEGY.md](docs/CACHING_STRATEGY.md) for details.

### Why cache-aside and not write-through?

Write-through caches every URL on creation, even ones never clicked. Cache-aside is lazy: only URLs that are actually read get cached. Cache hit rates are higher because memory only holds hot data. A Redis failure degrades to direct PostgreSQL reads — redirects still work.

### Why Kafka instead of writing analytics directly to PostgreSQL on each redirect?

A synchronous DB write on the redirect path adds latency to every click. At 10,000 redirects/sec, that's 10,000 concurrent DB writes — the pool saturates quickly. Kafka decouples it: the redirect returns in <5ms, the consumer aggregates clicks at its own pace. The consumer can lag without affecting user-facing latency. Additionally, raw events are stored in `click_events` table with UUID-based deduplication, providing exactly-once click counting semantics.

### What breaks at 10x load and how to fix it?

| Component | Bottleneck at 10x | Fix |
|-----------|------------------|-----|
| PostgreSQL writes | Connection pool exhaustion | PgBouncer connection pooler + read replicas |
| Redis | Hot key (one viral URL) | Per-process LRU cache (L1) in front of Redis |
| Single Express | CPU-bound JSON + routing | Horizontal scaling behind a load balancer |
| Kafka consumer | Slow aggregation lag | More partitions + more consumer instances (same `groupId`) |

## Project structure

```
url-shortener/
├── src/
│   ├── routes/
│   │   ├── shorten.js        # POST /shorten
│   │   ├── redirect.js       # GET /:code
│   │   └── stats.js          # GET /:code/stats
│   ├── services/
│   │   ├── shortener.js      # shorten + resolve logic
│   │   └── analytics.js      # stats query logic
│   ├── utils/
│   │   ├── base62.js         # encode/decode
│   │   └── snowflake.js      # Snowflake ID generator
│   ├── cache/
│   │   └── redis.js          # ioredis client + helpers
│   ├── db/
│   │   └── postgres.js       # pg pool + initSchema
│   ├── kafka/
│   │   ├── producer.js       # click event emitter
│   │   └── consumer.js       # click event aggregator
│   └── middleware/
│       ├── rateLimiter.js    # express-rate-limit + Redis store
│       └── errorHandler.js   # global 500 handler
├── frontend/
│   └── index.html            # minimal UI
├── tests/
│   ├── base62.test.js
│   ├── snowflake.test.js
│   ├── shorten.test.js
│   └── stats.test.js
├── docker-compose.yml
├── Dockerfile
└── .env.example
```
