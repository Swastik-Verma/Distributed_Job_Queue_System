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

## Handler Registry (Dispatch Pattern)

process_job() no longer contains job logic. It looks up job.type in a
HANDLERS dict and calls the registered function. Adding a job type means
writing a function and registering it — the worker loop never changes.

Same pattern Celery uses (task registry) and BullMQ (named processors).
Keeps the queue infrastructure generic: the worker knows how to claim,
retry, and record outcomes; it knows nothing about what any specific job
actually does.

Handlers validate their own payload, since `payload` is schemaless JSON
and nothing at the API layer enforces per-type shape.

## Transient vs Permanent Failures

Surfaced by the handler registry. Three distinct failure kinds now exist:
  - unknown job type      → permanent
  - missing payload field → permanent
  - flaky_test            → transient

Current retry design treats all failures identically: back off and retry
up to max_retries, then DEAD. For a permanent failure this wastes ~6
minutes of worker capacity across 4 attempts on something guaranteed to
fail identically every time — the handler will not exist on attempt 4
either.

**Correct design:** permanent failures should skip retry and go straight
to DEAD. Transient failures get the full exponential backoff.

**Planned implementation (Week 5):** a PermanentFailure exception class.
The worker catches it separately and routes directly to DEAD; any other
exception follows the normal retry path. The handler decides which kind
its failure is — the worker just honours the distinction.

**Interview framing:** retry policy is not one-size-fits-all. Retrying a
deterministic failure is not fault tolerance, it is wasted capacity plus
delayed failure reporting.


## Exponential Backoff

**Why not retry immediately:** three separate problems. Worker capacity is
burned spinning on a job that cannot yet succeed. The failing dependency
receives MORE load precisely when it is already overloaded (retry storm),
preventing recovery. And most transient failures need elapsed time to clear
— a restart, a healing route, a rate-limit window — so an instant retry asks
the same question before anything could have changed.

**The mechanism:** delay = base × (multiplier ^ retry_count), capped at a
maximum. Each failure is evidence the problem is more serious, so check less
often. Over the first 80s: fixed-5s retry makes 16 attempts, exponential
makes 4 — same coverage of the recovery window, a quarter of the load.

**Why exponential rather than linear:** over a 10-minute outage, linear
(5s, 10s, 15s, 20s...) makes ~15 attempts; exponential makes ~7. More
importantly, exponential adapts to UNKNOWN outage duration — it scales its
patience to the severity of the problem without being told how long the
outage will last. Linear treats a 5-second blip and a 5-hour outage with the
same escalation rate, which is right for neither.

**Why the cap:** uncapped, attempt 10 waits 5 × 2^9 ≈ 43 minutes.

**Chosen implementation:** computed exponential (not a lookup table), so it
scales to any max_retries value without editing a list. BASE_DELAY=5,
MULTIPLIER=2, MAX_DELAY=300. Cap applied AFTER jitter — a hard ceiling, so
no retry ever waits more than 5 minutes.

## Jitter and the Thundering Herd

**What backoff alone does NOT solve:** if a dependency goes down and 200 jobs
fail within the same window, every one computes an IDENTICAL delay (same
formula, same retry_count) and retries at the same instant. The recovering
service is hit by 200 simultaneous requests and falls over again — then the
whole synchronised wave repeats at the next delay.

Backoff spreads retries across TIME (successive attempts for the same job get
further apart). It does not spread them across JOBS, because every job
follows an identical schedule. Each wave is still a synchronised wall; the
walls just get further apart.

**Jitter** = deliberately adding randomness to a computed delay so jobs with
identical retry_count get different retry times. It breaks the
synchronisation that creates the herd.

**Strategies compared:**
  - Full jitter — random(0, delay). Maximum spread, but a 10s delay can
    collapse to 0.3s, defeating the point of waiting.
  - Equal jitter — delay/2 + random(0, delay/2). Guarantees a minimum wait.
  - Decorrelated — random(base, previous_delay × 3). Randomness compounds
    across attempts, so jobs diverge further each round. AWS's
    recommendation; used by gRPC. Best at massive scale where thousands of
    clients retry together and a fixed-width window is still too narrow.
  - CHOSEN: delay × random(0.5, 1.5). Equal-jitter spirit, one line, safe
    floor (never less than half the intended delay). Decorrelated is overkill
    at 3 workers and harder to explain.

**Backoff and jitter are two halves of one solution.** Jitter alone scatters
retries but clusters them at a short delay — no recovery time. Backoff alone
spaces the waves but each wave is a wall. Together: waves further apart AND
smeared out.

## Where the Delay Lives (Design Decision)

The worker must NOT sleep for the backoff duration. A 5-minute delay would
take that worker offline entirely; at 3 workers that is a third of capacity
idle for one failing job.

Instead: on failure, compute next_retry_at = now + delay, store it in
PostgreSQL, and move on immediately. A separate sweeper scans for FAILED jobs
whose next_retry_at has passed and re-queues them.

This is why next_retry_at existed in the schema from Day 8, and why the
sweeper is a Week 5 requirement rather than an extra — the backoff
calculation is only half the mechanism. Without the sweeper, no retry ever
actually fires.

## Retry Budget: What Happens When the Outage Outlasts It

max_retries creates a FINITE retry window. With 3 retries and exponential
backoff the total window is ~35 seconds. A 10-hour outage exhausts it in
under a minute and the job is marked DEAD — even though it was valid and the
service later recovered.

**Strategies for extending or compensating:**

1. Generous max_retries + delay cap. Once delay hits the 300s cap, every
   further retry costs one DB write per 5 minutes. 10 retries ≈ 25-minute
   window; 20 retries ≈ over an hour. Cheap — at ~72 retries over 6 hours
   against ~16,000 other jobs processed, it is well under 1% of capacity.
2. Per-job max_retries (already in the schema). A failed email might get 3
   attempts; a failed payment gets 20. Retry budget scales with the cost of
   giving up.
3. Manual retry endpoint (Day 34). Automated retries handle short
   self-healing outages; long outages needed human intervention anyway, so
   by the time the service is fixed a human already knows and can re-process
   the DEAD jobs. The trigger is "I just fixed the root cause" — NOT a timer.
4. Time-based retry budget instead of count-based. Keep retrying until N
   hours have elapsed, regardless of attempt count. AWS SQS does this
   (4-day default retention). Right at AWS scale where human intervention
   per incident is impossible; count-based is simpler and caps resource use
   predictably at this scale.

**The gap none of these fully close:** a short outage that self-heals AFTER
the retry budget is exhausted but BEFORE anyone notices. No human is
triggered, so manual retry never happens, and the jobs sit DEAD silently.
Mitigations: a generous enough budget to outlast typical blips; alerting on
DEAD-job volume; or an automatic resurrector — a periodic scan that re-queues
recently-DEAD jobs from scratch. A resurrector MUST exclude permanent
failures or it will cycle a never-succeeding job between DEAD and FAILED
forever — which is exactly what the PermanentFailure distinction (Day 27) is
for.

**Underlying acceptance:** no retry strategy guarantees 100% completion. The
goal is a recovery window long enough, and alerting loud enough, that a human
catches what matters before it is too late. Even SQS eventually deletes
messages after DLQ retention expires.

## Backoff Implementation Detail: The Silent Off-By-One

next_retry_at must be computed BEFORE retry_count is incremented.

  retry_count = 0 (no retries yet) → delay = 5 × 2^0 = 5s   correct
  computed after increment (=1)    → delay = 5 × 2^1 = 10s  wrong

Computing after the increment shifts the entire schedule one step up
(10s → 20s → 40s instead of 5s → 10s → 20s). Nothing crashes and the
delays still grow exponentially, so the bug is invisible in behaviour —
the documented schedule just silently stops matching reality. Off-by-one
errors in backoff logic don't throw; they produce a plausible-but-wrong
schedule. Only measuring actual delays catches them.

Verified empirically: five fail_test jobs failing within the same second
received five DIFFERENT next_retry_at values rather than an identical
5.00s. That spread IS the thundering-herd fix, visible directly in data.

## Jitter vs Cap Conflict at High Attempt Counts

Applying the delay cap AFTER jitter flattens all values to MAX_DELAY at
high attempts — defeating jitter entirely. Three jobs at attempt 7 all
get exactly 300s, creating the same synchronised wall jitter exists to
prevent.

Fix: cap BEFORE jitter, then jitter the capped value. This means some
delays can slightly exceed MAX_DELAY (up to 1.5×), but full jitter
spread is preserved at every attempt. Trade-off accepted: the
difference between a 300s and 450s retry at attempt 7 is insignificant,
but the difference between 10 synchronised retries and 10 scattered
retries is the thundering herd.

## Naive vs Aware Timestamps (Fixed)

Columns were TIMESTAMP WITHOUT TIME ZONE while the code wrote
datetime.now(timezone.utc). PostgreSQL silently stripped the offset.

Harmless while timestamps were only displayed. Would have become a real
bug when the sweeper compares next_retry_at <= NOW(): SQL's NOW()
returns server-local time (IST, UTC+5:30) against a UTC-stored value —
a 5.5-hour skew that would make every retry appear overdue and fire
immediately, silently disabling the entire backoff schedule.

Fixed by migrating columns to TIMESTAMPTZ and switching defaults from
the deprecated datetime.utcnow (naive) to datetime.now(timezone.utc)
(aware). PostgreSQL now stores and compares in a consistent timezone.

Interview framing: timezone bugs in retry/scheduling logic are silent —
no error, no crash, just every delay collapsing to zero. The only
defence is knowing the difference between naive and aware datetimes and
checking what your database column type actually stores.

## Transient vs Permanent Failure Routing

mark_failed() now routes three ways instead of one:
  PermanentFailure           → DEAD immediately (retry cannot help)
  retry_count > max_retries  → DEAD (gave up)
  otherwise                  → FAILED + next_retry_at

**Why permanence overrides the retry budget:** an unknown job type or a
missing payload field produces an identical result on every attempt. With
max_retries=3 and exponential backoff, retrying it burns ~35s of worker
capacity and delays the failure report, for a guaranteed-identical
outcome. Retrying a deterministic failure is not fault tolerance; it is
wasted capacity plus delayed reporting.

**Classification rule:** if the identical input could plausibly succeed
later, it is transient. If the answer is guaranteed identical, it is
permanent. Ambiguous cases (401 invalid credentials — a human might
rotate the key) default to TRANSIENT: over-classifying as permanent
throws away work that would have succeeded, which is the costlier error.

**max_retries semantics:** retry_count > max_retries means max_retries=3
allows 3 retries after the original attempt — 4 attempts total. Chosen
deliberately; >= would make it 3 attempts total.

**next_retry_at is nulled on DEAD** so the data never claims a retry is
pending for a terminal job, and so the sweeper cannot pick up a DEAD job
even if its status filter were wrong.

## Dead Letter Queue: Status Column, Not a Redis List

The original plan called for moving dead jobs to a separate Redis list
alongside status=DEAD. Rejected — same reasoning that rejected LMOVE to a
Redis "processing" list in Week 3.

A second Redis structure records a fact PostgreSQL already records, and
is strictly worse for the DLQ's actual purpose (inspection): a Redis list
holds bare UUIDs with no error messages, no grouping, no filtering, so
every entry has to be looked up in PostgreSQL anyway. It also
reintroduces the dual-write problem (this problem we had also solved by rejecting LMOVE to a Redis "processing" list in Week 3) — two systems, no shared transaction,
guaranteed drift on partial failure.

Third design decision resolved by the same principle: PostgreSQL is the
source of truth; Redis is a disposable pointer queue. A DLQ is defined by
isolation from normal processing plus preservation for inspection —
neither of which requires a separate queue.

## The Reconciliation Sweeper

Workers only see jobs whose IDs are in Redis. Once a worker pops an ID it is
gone from Redis permanently, so a job that then FAILS is invisible to the
entire pipeline — PostgreSQL knows it needs a retry, but nothing can find it.
The sweeper scans the source of truth and repairs that divergence.

**Status must reset to PENDING before the ZADD.** claim_job() requires
status='PENDING'. Requeueing a job that is still FAILED produces a silent
infinite loop: worker pops it, claim returns 0 rows, worker SKIPs, the ID is
consumed from Redis, the sweeper sees it due again and re-pushes it. No error
is ever raised and no work is ever done.

**Ordering: PostgreSQL first, Redis second.** A crash between the two leaves
the job PENDING but absent from Redis — which is exactly the stuck-PENDING
case the sweeper itself repairs. The reverse ordering produces the infinite
loop above instead. Same two writes; the ordering decides whether a crash is
self-healing or self-perpetuating.

**Batch limit exists to prevent a thundering herd of the sweeper's own
making.** An unbounded scan over a large backlog would fire every due retry in
one burst. Ordered oldest-first so a capped batch never permanently starves
anything.

**Scan interval sets the precision of the whole backoff schedule.** A job due
at 5s will not fire until the next scan, so the interval is added latency on
top of every computed delay.

**Fourth use of conditional update as compare-and-swap:** claim_job
(status='PENDING'), pick_job_from_tier (ZREM return value), mark_failed
routing, and now the sweeper's requeue (status='FAILED'). General rule: when
two actors might touch the same record, never check-then-act — make the write
conditional and read the affected-row count.

## Why Workers Scale But Sweepers Don't

**Workers are slow per job.** Each worker picks a job and spends 2+ seconds
processing it (sending an email, generating a PDF, calling an API). While
one worker is busy, other jobs wait. Adding workers means multiple jobs
process simultaneously — real parallelism, real throughput gain.

**Sweepers are fast per job.** The sweeper finds a due FAILED job, runs one
UPDATE (microseconds), one ZADD (microseconds), and moves to the next. A
single sweeper requeues 100 jobs in under a second — faster than the scan
interval. There is nothing to parallelise because the work finishes before
the next cycle begins.

**Can workers collide?** Yes — through weighted selection (ZRANGEBYSCORE +
ZREM), two workers can target the same job. But the system redirects the
loser cheaply: ZREM returns 0, the worker falls back to BZPOPMIN, and gets
a different job in microseconds. Collisions are rare and the redirect is
nearly free, so adding workers still increases throughput.

**Can sweepers collide?** Yes — but there is no redirect that helps. Multiple
sweepers run the same SELECT query and find the same rows. One requeues
everything in milliseconds. The others run conditional UPDATEs, get rows=0
on every single job, and accomplish nothing. The problem is not the race
itself — it is that the first sweeper already finished ALL the work before
the others even start their UPDATEs. There is nothing left to redirect to.

**The bottleneck determines whether scaling helps:**
  Workers:  bottleneck is processing time (seconds per job)  → scale helps
  Sweepers: bottleneck is scan interval (5s between cycles)  → scale is waste

**Docker implication (Week 7):** scale worker replicas freely, keep the
sweeper at replicas=1.

**Indexed (status, next_retry_at)** because the scan runs on a fixed interval
forever; without it, every cycle is a full table scan.

## Rejected: Redis Delayed Queue

A common alternative is a second sorted set scored by retry timestamp —
ZADD delayed_queue <unix_ts> <job_id>, then ZRANGEBYSCORE 0 <now>. Elegant,
and widely used in practice.

Rejected for the same reason as the Redis DLQ and the LMOVE processing list:
PostgreSQL already stores next_retry_at, so this is a second copy of existing
information written without a shared transaction — dual-write again. Fourth
design question settled by the same principle: PostgreSQL is the source of
truth, Redis is a disposable pointer queue.



## Sweeper Case 1: Stuck PENDING (Dual-Write Recovery)

POST /jobs commits to PostgreSQL, then ZADDs to Redis, with no transaction
spanning both. A lost second write leaves a job PENDING in the database with
no pointer in Redis — invisible to every worker, while the client already
received a 201. Identified in Week 2; this is the repair.

Detection: status=PENDING and updated_at older than PENDING_STUCK_SECONDS.

**The recovery is idempotent by construction.** The sweeper cannot cheaply
distinguish "PENDING and lost" from "PENDING and legitimately waiting in a
backlog" — and does not need to. Sorted sets enforce unique members, so ZADD
on a member already present just updates its score. A false positive costs one
wasted Redis write. (Same dedup property observed in the Day 22 experiments.)

**Age is measured on updated_at, not created_at,** and updated_at is refreshed
on requeue. With created_at, a job that legitimately waited in a backlog would
be re-swept every cycle forever, since its creation time never changes.

**Race with an in-flight claim:** a worker that has popped a job but not yet
run claim_job leaves it PENDING and absent from Redis — briefly identical to
the stuck case. If the sweeper requeues it, the worker claims it (→RUNNING)
and whoever pops the duplicate pointer fails claim_job and skips. The Day 17
conditional claim absorbs it. The 60s threshold makes this window practically
unreachable anyway.

## Sweeper Case 2: Orphaned RUNNING (Lease Recovery)

A worker that dies mid-job leaves the row RUNNING forever: the PENDING sweep
ignores it (wrong status) and its Redis pointer was already consumed.

**RUNNING is a LEASE, not a state.** A live worker holds it; updated_at older
than RUNNING_LEASE_SECONDS means the holder is presumed dead, so the job is
reset to PENDING and re-queued. Equivalent to SQS visibility timeout and
Celery's visibility_timeout.

**The threshold must exceed the longest realistic job duration.** There are no
heartbeats, so a slow-but-alive job is indistinguishable from a dead worker.
60s against ~3s handlers gives 20× headroom. The proper fix is a heartbeat —
the worker refreshes updated_at while working — which becomes necessary once
job durations vary widely. Deliberately not built: uniform short jobs here.

**Conditional update as the final safety net.** The whole premise is a guess.
UPDATE ... WHERE status='RUNNING' means a worker that actually finished (and
wrote SUCCESS) causes the reset to match zero rows, and the sweeper backs off.
Fifth use of conditional-write-as-compare-and-swap.

**retry_count IS incremented on orphan recovery — deliberate trade-off.**
Not incrementing feels fairer (the job didn't fail; its worker died). But a
POISON PILL — a job whose payload crashes whatever handler runs it — would
then loop forever, killing a worker per cycle with nothing to stop it.
Incrementing bounds the damage: after max_retries recoveries the job reaches
DEAD and a human can inspect it. Cost: an occasional unfair strike when a
worker dies for unrelated reasons (deploy, container restart). The cheaper
mistake. SQS makes the same call — an expired visibility timeout counts as a
receive and eventually routes the message to the DLQ.

**Recovery necessarily re-runs completed work.** A worker that finished the
side effect and died before writing SUCCESS has that side effect repeated.
The queue cannot prevent this; only idempotent handlers can. At-least-once
delivery is the achievable target, not exactly-once.

## Sweep Ordering and Indexing

Due retries are swept first — the common path, running constantly. The two
recovery cases are exception handling for rare events; if a batch limit is
ever reached, the normal path should not be crowded out by them.

Added index on (status, updated_at) — both new sweeps filter on that pair
every SWEEPER_INTERVAL seconds, alongside the existing
(status, next_retry_at) index for due retries.