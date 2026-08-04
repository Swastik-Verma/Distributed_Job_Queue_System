import os
import time
from uuid import UUID
from datetime import datetime, timezone

import redis
from dotenv import load_dotenv

from app.database import SessionLocal
from app.models import Job, JobStatus

load_dotenv()

QUEUE_NAMES = ["redis_queue:HIGH", "redis_queue:MEDIUM", "redis_queue:LOW"]


def process_job(job):
    """Pretend to do the actual work. Real handlers come later."""
    print(f"    type={job.type}  priority={job.priority.value}")
    print(f"    payload={job.payload}")
    time.sleep(2)


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


def run_worker():
    pid = os.getpid()

    worker_redis = redis.Redis(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        decode_responses=True,
        socket_timeout=30,
        socket_connect_timeout=5,
    )

    print(f"[worker {pid}] started, watching {len(QUEUE_NAMES)} queues")

    while True:
        result = worker_redis.brpop(QUEUE_NAMES, timeout=5)

        if result is None:
            continue

        queue_name, job_id = result
        print(f"[worker {pid}] picked {job_id} from {queue_name}")

        db = SessionLocal()
        try:
            job = claim_job(db, UUID(job_id))

            if job is None:
                continue

            print(f"[worker {pid}] CLAIMED {job_id} — status is now RUNNING")
            process_job(job)
            print(f"[worker {pid}] finished {job_id}\n")
        finally:
            db.close()


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        print("\n[worker] stopped")