import os
import time
from uuid import UUID

import redis
from dotenv import load_dotenv

from app.database import SessionLocal
from app.models import Job

load_dotenv()

QUEUE_NAMES = ["redis_queue:HIGH", "redis_queue:MEDIUM", "redis_queue:LOW"]


def process_job(job):
    """Pretend to do the actual work. Real handlers come later."""
    print(f"    type={job.type}  priority={job.priority.value}")
    print(f"    payload={job.payload}")
    time.sleep(2)


def run_worker():
    pid = os.getpid()

    # Worker creates its own Redis connection — not shared with the API
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
            job = db.query(Job).filter(Job.id == UUID(job_id)).first()

            if job is None:
                print(f"[worker {pid}] WARNING: {job_id} not found in PostgreSQL")
                continue

            process_job(job)
            print(f"[worker {pid}] finished {job_id}\n")
        finally:
            db.close()


if __name__ == "__main__":
    try:
        run_worker()
    except KeyboardInterrupt:
        print("\n[worker] stopped")