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