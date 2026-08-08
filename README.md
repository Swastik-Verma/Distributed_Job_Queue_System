# Distributed Job Queue System

A background job processing system built from scratch in Python — the
kind of infrastructure that sits behind "your order is confirmed, email
on its way." Similar in spirit to Celery or BullMQ.

**Status:** In development (Day 25 of 60)

## The problem

When you place an order, the app must save it and respond instantly.
But it also needs to send a confirmation email, an SMS, notify the
restaurant. Doing all that inline would make the user wait seconds.

Instead, those side effects are pushed onto a queue and handled by
background workers. This project is that queue system.

## Architecture
```mermaid
graph TD
    Client[Client] --> FastAPI[FastAPI]
    
    FastAPI --> PostgreSQL[(PostgreSQL<br>source of truth)]
    FastAPI --> Redis[(Redis<br>job ID queues)]
    
    PostgreSQL --> WorkerPool[Worker pool]
    Redis --> WorkerPool
```

PostgreSQL holds the full job record and is the single source of truth.
Redis holds only job IDs, acting as a lightweight waiting line. Workers
pop an ID from Redis, then load the full job from PostgreSQL.

## Tech stack

- **Python 3 / FastAPI** — API layer
- **PostgreSQL / SQLAlchemy** — job persistence
- **Redis** — job queues (Lists now, Sorted Sets for priority later)
- **Pydantic** — request validation

## Implemented so far

- [x] FastAPI server with health checks
- [x] PostgreSQL connection via SQLAlchemy ORM
- [x] Redis connection
- [x] `jobs` table schema with status/priority enums, retry tracking
- [x] `POST /jobs` — submit a job (201), persisted as `PENDING`
- [x] `GET /jobs/{id}` — fetch job status (200/404)
- [x] Pydantic request/response schemas with case-insensitive priority
- [x] Project restructured: `app/` package with `routers/` for endpoints
- [x] 3 worker processes running in parallel via multiprocessing launcher
- [x] Conditional claim prevents duplicate processing
- [x] Jobs marked SUCCESS or FAILED after processing, errors captured
- [x] Priority queues migrated from 3 Redis Lists to 1 Sorted Set
- [x] Weighted round-robin selection prevents priority starvation
      (verified under load — see below)
- [x] Priority weights configurable in one place (`app/config.py`)
- [x] `GET /jobs/stats/queue` — live queue depth by priority tier

## Roadmap

- [ ] `GET /jobs/{id}` — job status lookup
- [ ] Worker processes via Python multiprocessing
- [ ] Priority queues using Redis Sorted Sets
- [ ] Retry with exponential backoff + Dead Letter Queue
- [ ] Orphan/reconciliation sweeper
- [ ] Live dashboard over WebSockets
- [ ] Docker Compose for the full stack
- [ ] Deployment

## Data model

The `jobs` table:

| Column | Type | Purpose |
|---|---|---|
| `id` | UUID | Generated client-side friendly ID; avoids collisions across services |
| `type` | String | Which handler runs, e.g. `send_email` |
| `payload` | JSON | Handler input — schemaless so one table serves all job types |
| `status` | Enum | PENDING / RUNNING / SUCCESS / FAILED / DEAD |
| `priority` | Enum | HIGH / MEDIUM / LOW |
| `retry_count` | Integer | Attempts so far |
| `max_retries` | Integer | Per-job retry limit |
| `next_retry_at` | Timestamp | Earliest time this may be retried |
| `error_message` | Text | Most recent failure reason |
| `created_at` / `updated_at` | Timestamp | Bookkeeping |

`FAILED` is transient (a retry is pending); `DEAD` is terminal.

## Design notes

**Why UUIDs?** In a distributed system, IDs may be generated in several
places at once. UUIDs remove the need for a central counter.

**Why a JSON payload?** Different job types need entirely different
inputs. A schemaless column supports new job types without migrations.

**Why store only the ID in Redis?** Storing the full job in both systems
would mean two copies that can drift apart. Redis is a pointer;
PostgreSQL is the truth.

See [NOTES.md](./NOTES.md) for known trade-offs and failure modes.

## Running locally

Requires PostgreSQL and Redis running locally.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env      # then fill in your credentials

uvicorn app.main:app --reload
```

Interactive API docs: http://localhost:8000/docs
 
## Priority Queues with Starvation Prevention

Jobs are queued in a single Redis Sorted Set where the score encodes priority (HIGH=1, MEDIUM=5, LOW=10). Workers don't simply take the lowest score — that would starve LOW-priority jobs under sustained HIGH load. Instead, each worker walks a weighted round-robin cycle (6 HIGH : 3 MEDIUM : 1 LOW), targeting a specific tier on each pick 
via ZRANGEBYSCORE + ZREM. If the scheduled tier is empty, the worker falls back to a global BZPOPMIN rather than idling.

Verified under load: with 40 HIGH jobs and 3 LOW jobs pre-loaded, 2 out of 3 LOW jobs completed while HIGH jobs were still being processed. LOW jobs waited ~29s vs HIGH's ~11-37s range — slower by design, but not starved.

Balanced load test (60 HIGH / 30 MEDIUM / 30 LOW):

| Priority | Jobs | Avg wait | Max wait | Direct hits |
|----------|------|----------|----------|-------------|
| HIGH     | 60   | 78s      | 106s     | 42 (57%)    |
| MEDIUM   | 30   | 93s      | 116s     | 21 (29%)    |
| LOW      | 30   | 115s     | 130s     | 10 (14%)    |

Target ratio 60/30/10 — measured 57/29/14. All tiers completed.