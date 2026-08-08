import os
import time
from uuid import UUID
from datetime import datetime, timezone
from app.config import REDIS_QUEUE_KEY, PRIORITY_SCORES, WEIGHT_CYCLE
from worker.handlers import HANDLERS

import redis
from dotenv import load_dotenv

from app.database import SessionLocal
from app.models import Job, JobStatus


load_dotenv()

# QUEUE_NAMES = ["redis_queue:HIGH", "redis_queue:MEDIUM", "redis_queue:LOW"]

# WEIGHT_CYCLE = (
#     ["HIGH"] * 6 +
#     ["MEDIUM"] * 3 +
#     ["LOW"] * 1
# )


def pick_job_from_tier(worker_redis, tier):
    """Try to pop one job whose score matches this tier."""
    score = PRIORITY_SCORES[tier]

    candidates = worker_redis.zrangebyscore(
        REDIS_QUEUE_KEY, score, score, start=0, num=1
    )
    if not candidates:
        return None

    job_id = candidates[0]
    removed = worker_redis.zrem(REDIS_QUEUE_KEY, job_id)

    return job_id if removed == 1 else None

# def process_job(job, pid):
#     """Simulate doing actual work. Real handlers come later."""
#     if job.type == "fail_test":
#         raise Exception("Simulated failure: email server unreachable")

#     print(f"[worker {pid}] : type={job.type},  priority={job.priority.value}, payload={job.payload}")
#     time.sleep(2)
def process_job(job, pid):
    """Dispatch the job to its registered handler."""
    handler = HANDLERS.get(job.type)

    if handler is None:
        raise ValueError(f"No handler registered for job type: '{job.type}'")

    print(f"[worker {pid}] : type={job.type}, priority={job.priority.value}, payload={job.payload}")
    return handler(job.payload)



def claim_job(db, job_id):
    """Try to claim the job by setting status to RUNNING.
    Returns the job if claimed, None if someone else got it first."""
    rows_updated = (
        db.query(Job)
        .filter(Job.id == job_id, Job.status == JobStatus.PENDING)
        .update({
            Job.status: JobStatus.RUNNING,
            Job.updated_at: datetime.now(timezone.utc),
        })
    )
    db.commit()

    if rows_updated == 0:
        print(f"[worker {os.getpid()}] SKIP {job_id} — already claimed or not PENDING")
        return None

    return db.query(Job).filter(Job.id == job_id).first()


def mark_success(db, job):
    """Mark job as completed successfully."""
    job.status = JobStatus.SUCCESS
    job.updated_at = datetime.now(timezone.utc)
    db.commit()


def mark_failed(db, job, error):
    """Mark job as failed and save the error message."""
    job.status = JobStatus.FAILED
    job.error_message = str(error)
    job.retry_count += 1
    job.updated_at = datetime.now(timezone.utc)
    db.commit()


def run_worker():
    pid = os.getpid()
    cycle_index = 0

    worker_redis = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        decode_responses=True,
        socket_timeout=30,
        socket_connect_timeout=5,
    )

    print(f"[worker {pid}] started, watching queue: {REDIS_QUEUE_KEY}")

    while True:
        tier = WEIGHT_CYCLE[cycle_index % len(WEIGHT_CYCLE)]
        cycle_index += 1

        job_id = pick_job_from_tier(worker_redis, tier)

        if job_id is None:
            result = worker_redis.bzpopmin(REDIS_QUEUE_KEY, timeout=5)
            if result is None:
                continue
            _, job_id, score = result
            print(f"[worker {pid}] picked {job_id} (scheduled tier={tier}, but empty — fell back to global lowest score={score})")
        else:
            print(f"[worker {pid}] picked {job_id} (scheduled tier={tier}, direct hit)")
            
        db = SessionLocal()
        try:
            job = claim_job(db, UUID(job_id))

            if job is None:
                continue

            print(f"[worker {pid}] CLAIMED {job_id} — status is now RUNNING")

            try:
                process_job(job, pid)
                mark_success(db, job)
                print(f"[worker {pid}] ✓ SUCCESS {job_id}\n")
            except Exception as e:
                mark_failed(db, job, e)
                print(f"[worker {pid}] ✗ FAILED {job_id} — {e}\n")

        finally:
            db.close()


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        print("\n[worker] stopped")