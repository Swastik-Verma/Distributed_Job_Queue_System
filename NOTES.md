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


## Scaling workers
Workers are stateless — each runs the same loop, creates its own
connections, coordinates only through Redis (BRPOP) and PostgreSQL
(conditional claim). Scaling is changing the worker count; no code
changes required. The launcher uses multiprocessing.Process to fork
N identical workers from a single command.

The conditional claim was tested under real concurrency: three workers
racing for the same job ID, with only one succeeding. The losers
detect 0 rows updated and skip gracefully.

## Redis Sorted Sets vs Lists for Priority Queues

**Problem:** Three separate Redis Lists (HIGH/MEDIUM/LOW) with BRPOP 
checking left-to-right causes priority starvation — LOW jobs never run 
if HIGH jobs keep arriving.

**Solution:** One Redis Sorted Set where each member is a job ID and 
the score represents priority (lower = more urgent). BZPOPMIN replaces 
BRPOP as the blocking pop command.

**Key behavioral differences:**
- Sorted Sets enforce unique members — ZADD on an existing member 
  updates its score instead of adding a duplicate. Free dedup at the 
  queue level.
- ZPOPMIN alone still causes starvation (same as strict ordering). 
  Weighted selection logic in the worker is needed to fix it.
- Score gaps (1/5/10 instead of 1/2/3) leave room for future priority 
  tiers without restructuring.

**Commands:** ZADD (add), BZPOPMIN (blocking pop), ZRANGE WITHSCORES 
(inspect), ZCARD (count), ZRANGEBYSCORE (filter by priority tier).

## Starvation Fix: Weighted Selection vs Ageing

Two approaches to prevent priority starvation:

**Weighted selection:** Worker randomly picks a priority tier each cycle 
using fixed weights (e.g. 60% HIGH, 30% MEDIUM, 10% LOW). Guarantees 
throughput share per tier. Stateless — no background processes needed.

**Ageing:** A job's effective priority increases the longer it waits, 
eventually matching higher tiers. Guarantees latency — every job 
eventually reaches the front. Requires either periodic score 
recalculation (extra sweeper + Redis writes) or a time-encoding formula 
that's hard to tune correctly.

**Chose weighted selection** because it's stateless, deterministic, and 
gives a clear answer to "what share of capacity does each tier get?" 
Ageing is better suited to OS process schedulers where individual task 
starvation matters more than tier-level throughput.

## Weighted Selection Implementation

**Mechanism:** Worker maintains a local `cycle_index` and walks a fixed 
weight cycle (6xHIGH, 3xMEDIUM, 1xLOW = 60/30/10 split) to decide which 
tier to target on each pop, instead of always taking the global minimum 
score.

**Popping a specific tier:** Redis has no atomic "pop min within score 
range" command, so this is simulated with ZRANGEBYSCORE (read a 
candidate) + ZREM (attempt removal). ZREM's return value (1 or 0) acts 
as a compare-and-swap check — same pattern as the conditional claim 
already used against PostgreSQL (Day 17), just applied to Redis. At 
most one worker can successfully ZREM a given member.

**Fallback for empty scheduled tier:** If the cycle says "try HIGH" but 
HIGH is empty, the worker doesn't idle — it falls back to BZPOPMIN 
(global lowest score) so it stays productive whenever ANY work exists. 
Weighting is only meaningful when multiple tiers have jobs simultaneously 
waiting; an empty tier should never block a worker from picking up 
other available work.

**Why round-robin over random weighted choice:** Deterministic — 
pop #7 in a worker's local sequence is always predictable, making the 
mechanism easy to test and reason about, versus random.choices which 
gives the same long-run ratio but with unpredictable short-term variance.

## Idempotency: The Deliberately Unbuilt Third Layer

**The gap:** A worker completes real work (sends an email, charges a card) 
and then crashes before writing SUCCESS to PostgreSQL. The job stays 
RUNNING. The Week 5 sweeper detects the stale RUNNING status and 
re-queues it — correct behavior for a crashed worker — but the side 
effect now happens twice.

**Why the existing two layers don't cover it:**
- Conditional claim (UPDATE ... WHERE status='PENDING') catches 
  CONCURRENT duplicates — two workers racing for the same job.
- ZREM return-value check catches the same race at the Redis layer.
- Neither catches SEQUENTIAL duplicates — the same job legitimately 
  running twice, at different times, because the first run was never 
  recorded as complete.

**Key insight:** the claim protects a database row. It cannot un-send 
an email.

**Why it wasn't built:**
1. process_job() is a stub (print + sleep). No real side effects exist, 
   so there is nothing to protect and no way to test the protection.
2. The correct implementation is per-handler, not generic. send_email 
   needs a dedup key; charge_payment needs the provider's own 
   idempotency key (so THEIR system rejects the duplicate); 
   generate_pdf and recalculate_stats are naturally idempotent and 
   need nothing.
3. Wrong layer. Idempotency is application logic, not queue 
   infrastructure. Celery and BullMQ make the same split — they 
   guarantee at-least-once delivery and explicitly document that the 
   task author is responsible for idempotent handlers.
4. Time budget — invisible in a demo, untestable against stubs, 
   competing with retry/DLQ/dashboard/Docker work.

**How it would be implemented:** a dedup key checked before the side 
effect and written after it, e.g.
    key = f"email_sent:{job.id}"
    if redis.get(key): return          # already done, skip
    send_email(...)
    redis.set(key, "1", ex=86400)
For external APIs, pass the key to the provider (Stripe-style) so the 
duplicate is rejected server-side rather than client-side.

**Underlying principle:** exactly-once delivery is not achievable 
across a queue and a database. The achievable target is at-least-once 
delivery plus idempotent handlers.



## Starvation Prevention: Measured Results

**Test setup:** 3 LOW jobs submitted first, then 40 HIGH jobs — all 
pre-loaded into Redis before workers started, creating a 43-job backlog 
with a 40:3 HIGH-to-LOW imbalance.

**Without weighted selection (plain BZPOPMIN):** all LOW jobs would wait 
until every HIGH job finishes (~37+ seconds). Starvation guaranteed 
under sustained HIGH load.

**With weighted selection (6:3:1 cycle):** 2 out of 3 LOW jobs completed 
at ~29 seconds while HIGH jobs were still being processed (last HIGH 
finished at ~37s). The third LOW job was picked up last via fallback 
after HIGH was drained.

**Measured wait times:**
- HIGH jobs: 11s (fastest) to 37s (slowest)
- LOW jobs: 29s (rescued by weighted selection) to 39s (last job, fallback)
- LOW was slower than HIGH — by design — but not starved.

**Important nuance:** the 6:3:1 ratio describes the worker's INTENTION 
(which tier to target each turn), not the OUTCOME. Outcomes depend on 
what's actually available. MEDIUM turns always fell back (zero MEDIUM 
jobs submitted). Some LOW turns lost ZREM races to other workers and 
fell back. The ratio is followed in intent; demand determines results.

**Direct hits vs fallbacks (from grep):**
- 21 direct hits on HIGH, 2 on LOW, 0 on MEDIUM
- 22 total direct hits, 20 fallbacks
- Fallbacks are expected and correct — they keep workers productive 
  when their scheduled tier is empty.

**Why the load must exceed worker capacity:** if workers drain the queue 
as fast as it fills, no backlog forms, and weighted vs unweighted 
strategies produce identical results. Starvation only becomes observable 
under sustained backlog pressure.


## Weighted Selection: Balanced Load Measurement

Day 25's test (40 HIGH / 0 MEDIUM / 3 LOW) proved starvation prevention
but could not measure the throughput ratio — MEDIUM had no demand and
LOW ran dry immediately, so most non-HIGH turns fell back.

Re-tested with balanced demand (60 HIGH / 30 MEDIUM / 30 LOW) so no
tier runs dry mid-run. With all tiers populated, direct hits track the
configured 6:3:1 split and the fallback rate drops sharply.

**Key insight:** fallback frequency is a signal of tier IMBALANCE, not
a defect. High fallback rate means some tier has no demand; low fallback
rate means all tiers are competing and the weighting is doing real work.

**Ratio is intention, not outcome.** The cycle guarantees which tier a
worker TARGETS. What it actually gets depends on available demand. A
weighting scheme cannot manufacture jobs for an empty tier — nor should
it idle a worker to preserve a ratio.

## Queue Observability

Added GET /jobs/stats/queue reporting total queue depth and per-tier depth via ZCARD and ZCOUNT. Read-only — counts without popping.

Note that Redis queue depth and PostgreSQL PENDING count are two independent views that SHOULD agree. Divergence indicates the dual-write failure (committed to PostgreSQL, never reached Redis). Once the Week 5 sweeper exists, comparing these two numbers becomes a system health check.

**FastAPI routing note:** the endpoint is /jobs/stats/queue rather than /jobs/stats because a single-segment route would be matched by the existing /jobs/{job_id} path-param route and fail UUID validation with a 422.