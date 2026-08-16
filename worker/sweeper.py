"""
Reconciliation sweeper.

Workers only see jobs whose IDs are in Redis. Anything that falls out of
Redis — or never made it there — is invisible to them forever. The sweeper
scans PostgreSQL (the source of truth) and repairs that divergence.
"""
import os
import time
from datetime import datetime, timezone, timedelta

import redis
from dotenv import load_dotenv

from app.config import (
    REDIS_QUEUE_KEY,
    PRIORITY_SCORES,
    SWEEPER_INTERVAL,
    SWEEPER_BATCH_SIZE,
    PENDING_STUCK_SECONDS,
    RUNNING_LEASE_SECONDS,
)
from app.database import SessionLocal
from app.models import Job, JobStatus

load_dotenv()


def log(message):
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[sweeper {stamp}] {message}")


def requeue_due_retries(db, sweeper_redis):
    """Find FAILED jobs whose retry time has arrived and put them back."""
    now = datetime.now(timezone.utc)

    due = (
        db.query(Job)
        .filter(
            Job.status == JobStatus.FAILED,
            Job.next_retry_at.isnot(None),
            Job.next_retry_at <= now,
        )
        .order_by(Job.next_retry_at)
        .limit(SWEEPER_BATCH_SIZE)
        .all()
    )

    requeued = 0
    for job in due:
        rows = (
            db.query(Job)
            .filter(Job.id == job.id, Job.status == JobStatus.FAILED)
            .update({
                Job.status: JobStatus.PENDING,
                Job.next_retry_at: None,
                Job.updated_at: datetime.now(timezone.utc),
            })
        )
        db.commit()

        if rows == 0:
            continue

        score = PRIORITY_SCORES[job.priority.value]
        sweeper_redis.zadd(REDIS_QUEUE_KEY, {str(job.id): score})
        requeued += 1

        attempt = job.retry_count + 1
        log(f"requeued {job.id} (attempt {attempt}/{job.max_retries + 1}, {job.priority.value})")

    return requeued


def requeue_stuck_pending(db, sweeper_redis):
    """Re-queue PENDING jobs that appear to have lost their Redis pointer."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=PENDING_STUCK_SECONDS)

    stuck = (
        db.query(Job)
        .filter(Job.status == JobStatus.PENDING, Job.updated_at < cutoff)
        .order_by(Job.updated_at)
        .limit(SWEEPER_BATCH_SIZE)
        .all()
    )

    requeued = 0
    for job in stuck:
        score = PRIORITY_SCORES[job.priority.value]
        sweeper_redis.zadd(REDIS_QUEUE_KEY, {str(job.id): score})

        db.query(Job).filter(Job.id == job.id, Job.status == JobStatus.PENDING).update(
            {Job.updated_at: datetime.now(timezone.utc)}
        )
        db.commit()

        requeued += 1
        log(f"recovered stuck PENDING {job.id} ({job.priority.value})")

    return requeued


def recover_orphaned_running(db, sweeper_redis):
    """Reset jobs stuck in RUNNING past the lease window."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=RUNNING_LEASE_SECONDS)

    orphaned = (
        db.query(Job)
        .filter(Job.status == JobStatus.RUNNING, Job.updated_at < cutoff)
        .order_by(Job.updated_at)
        .limit(SWEEPER_BATCH_SIZE)
        .all()
    )

    recovered = 0
    for job in orphaned:
        rows = (
            db.query(Job)
            .filter(Job.id == job.id, Job.status == JobStatus.RUNNING)
            .update({
                Job.status: JobStatus.PENDING,
                Job.retry_count: Job.retry_count + 1,
                Job.error_message: "Worker died mid-job (lease expired)",
                Job.updated_at: datetime.now(timezone.utc),
            })
        )
        db.commit()

        if rows == 0:
            continue

        score = PRIORITY_SCORES[job.priority.value]
        sweeper_redis.zadd(REDIS_QUEUE_KEY, {str(job.id): score})

        recovered += 1
        log(f"recovered orphaned RUNNING {job.id} (attempt {job.retry_count + 2})")

    return recovered


def run_sweeper():
    pid = os.getpid()

    sweeper_redis = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        decode_responses=True,
        socket_timeout=30,
        socket_connect_timeout=5,
    )

    log(f"started (PID {pid}), scanning every {SWEEPER_INTERVAL}s")

    while True:
        db = SessionLocal()
        try:
            retries = requeue_due_retries(db, sweeper_redis)
            stuck = requeue_stuck_pending(db, sweeper_redis)
            orphans = recover_orphaned_running(db, sweeper_redis)

            if retries or stuck or orphans:
                log(f"cycle complete — retries={retries} stuck={stuck} orphans={orphans}")
        except Exception as e:
            log(f"ERROR during scan: {e}")
        finally:
            db.close()

        time.sleep(SWEEPER_INTERVAL)


if __name__ == "__main__":
    try:
        run_sweeper()
    except KeyboardInterrupt:
        log("stopped")