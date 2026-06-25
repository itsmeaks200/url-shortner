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
| `alias` | string | ❌ | 3–50 chars, alphanumeric + hyphens |
| `ttl_days` | number | ❌ | Expiry in days from now |

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
| `301` | Permanent redirect to `long_url` |
| `404` | Code not found |
| `410` | URL has expired |

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

### Why 301 and not 302?

`301` (permanent) tells browsers and CDNs to cache the redirect. Future visits to the same short URL never hit the server — the browser redirects locally. The trade-off: if the destination URL changes, cached clients won't see it. Acceptable here because URLs are immutable after creation.

### Why cache-aside and not write-through?

Write-through caches every URL on creation, even ones never clicked. Cache-aside is lazy: only URLs that are actually read get cached. Cache hit rates are higher because memory only holds hot data. A Redis failure degrades to direct PostgreSQL reads — redirects still work.

### Why Kafka instead of writing analytics directly to PostgreSQL on each redirect?

A synchronous DB write on the redirect path adds latency to every click. At 10,000 redirects/sec, that's 10,000 concurrent DB writes — the pool saturates quickly. Kafka decouples it: the redirect returns in <5ms, the consumer aggregates clicks at its own pace. The consumer can lag without affecting user-facing latency.

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
