# Engineering notes

Failure modes and trade-offs identified while building this system.
Kept here so design reasoning isn't lost, and for interview prep.

## Dual-write problem
`POST /jobs` writes to PostgreSQL, then to Redis. No transaction spans
both. If Redis fails after the DB commit, the job is persisted as
PENDING but invisible to workers — and the client already got a 200.
Dangerous specifically because success was already promised.

Fixes:
- **Transactional outbox** — insert the job and an outbox row in one
  transaction; a relay process pushes to Redis. Prevents the problem
  entirely. Used by Uber, Shopify, most payment infra.
- **Reconciliation sweeper** — periodically re-queue jobs stuck in
  PENDING. Detects and repairs instead of preventing.

Chosen: sweeper (Week 5). Less machinery, and jobs are idempotent
anyway, so the duplicate risk it introduces is acceptable.

## At-least-once delivery
Every recovery mechanism here can re-deliver a job. Exactly-once is not
achievable across a queue and a database. The system targets
at-least-once and relies on idempotent handlers.

## Duplicate suppression — two layers
- **Conditional claim (compare-and-swap):**
  `UPDATE jobs SET status='RUNNING' WHERE id=? AND status='PENDING'`
  A second worker gets 0 rows and backs off. Cheap, generic, catches
  *concurrent* duplicates.
- **Idempotency:** handler-level dedup (e.g. "already emailed order X?",
  Stripe-style idempotency keys). Catches *sequential* duplicates — a
  worker that completed the side effect then died before marking
  SUCCESS.

Claim protects a database row; it cannot un-send an email. Both layers
are needed. Naturally idempotent jobs (overwrites, recalculations) need
only the claim.

## Orphaned RUNNING jobs
A worker that dies mid-job leaves the row in RUNNING forever — the
PENDING sweeper won't see it, and its Redis entry is already consumed.

Treat RUNNING as a lease, not a state: reset jobs whose `updated_at` is
stale past a threshold. Risk: slow-but-alive jobs get killed. Mitigated
by heartbeats (worker refreshes `updated_at` periodically) or per-job
timeouts.

Equivalent to SQS visibility timeout / Celery `visibility_timeout`.

## Client-side retries
If the server dies after commit but before responding, the client sees a
timeout and retries — creating a duplicate job. Real APIs accept a
client-supplied idempotency key on submission. Possible Week 8 addition.

## Redis Lists vs Sorted Sets
Currently three Lists (HIGH/MEDIUM/LOW) with workers checking in order.
Works, but means maintaining three queues. Week 4 moves to a single
Sorted Set where the priority is the score, so Redis handles ordering.


## Why multiprocessing, not threads
CPython's GIL serialises bytecode execution, so threads give no
speedup for CPU-bound work. The GIL *is* released during I/O, so
threads would work for email/API jobs — but processes were chosen for:
fault isolation (one crash doesn't kill the pool), uniform handling of
CPU-heavy job types (PDF generation, image resizing), and parity with
Celery's default prefork pool.

Trade-off: ~30-50MB per process vs KBs per thread. Fine at 3 workers,
wrong choice at 200. A purely I/O-bound high-concurrency system should
use asyncio.

## Fork safety
Processes do not share memory — coordination must happen through Redis
or PostgreSQL, never through Python variables.

Connections must never cross the fork boundary. A forked child inherits
the parent's open sockets, so parent and child would interleave bytes on
the same TCP connection. Each worker creates its own Redis client and
SQLAlchemy engine *inside* the process.

## Why RPOP is safe for multiple workers
Redis is single-threaded and processes commands serially, so concurrent
RPOPs are atomic — no two workers receive the same value. This does not
cover worker death after the pop: the item is gone from Redis with no
record it was taken. PostgreSQL status tracking covers that gap.

## Busy-wait vs blocking pop
RPOP in a tight loop returns None instantly, spinning the CPU and
flooding Redis with pointless requests. BRPOP blocks at the OS level —
zero CPU while idle, sub-millisecond wake-up when a job arrives. A short
timeout (5s) is used rather than blocking forever so the process stays
responsive to signals and can do periodic housekeeping.

## Priority via BRPOP key order
BRPOP accepts multiple keys and checks them left to right, so passing
[HIGH, MEDIUM, LOW] gives strict priority in a single call.

Known flaw: strict priority means a continuous stream of HIGH jobs
starves LOW entirely. Real systems mitigate with weighted selection or
ageing (raising a job's effective priority the longer it waits).
Revisited in Week 4.

## Jobs are invisible between BRPOP and the status write
BRPOP removes the ID from Redis atomically, but until the worker writes
RUNNING to PostgreSQL, no system records that the job was taken. A crash
in that window leaves the job PENDING forever with no queue entry.

Redis's own recommended fix is RPOPLPUSH/LMOVE — atomically move the ID
to a "processing" list so it is never in limbo, with a reaper returning
stale entries. This project uses the PostgreSQL status column for that
tracking instead: it is already durable and already the source of truth,
so mirroring in-flight state in Redis adds little. The trade-off is a
small window where the ID exists in neither place.