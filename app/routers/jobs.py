from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from datetime import datetime, timezone
from app.database import get_db
from app.models import Job, JobStatus
from app.schemas import JobCreate, JobResponse
from app.redis_client import redis_client
from app.config import REDIS_QUEUE_KEY, PRIORITY_SCORES

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobResponse, status_code=201)
def create_job(job_data: JobCreate, db: Session = Depends(get_db)):
    # Save job to PostgreSQL
    new_job = Job(
        type=job_data.type,
        priority=job_data.priority,
        payload=job_data.payload,
        status=JobStatus.PENDING,
        max_retries=job_data.max_retries
    )
    db.add(new_job)
    db.commit()
    db.refresh(new_job)

    # Push job ID into the matching Redis priority queue
    score = PRIORITY_SCORES[new_job.priority.value]
    redis_client.zadd(REDIS_QUEUE_KEY, {str(new_job.id): score})

    return new_job

@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: UUID, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return job


@router.get("/stats/queue")
def queue_stats():
    """Current queue depth, broken down by priority tier."""
    total = redis_client.zcard(REDIS_QUEUE_KEY)

    by_priority = {}
    for tier, score in PRIORITY_SCORES.items():
        by_priority[tier] = redis_client.zcount(REDIS_QUEUE_KEY, score, score)

    return {
        "queue_key": REDIS_QUEUE_KEY,
        "total_queued": total,
        "by_priority": by_priority,
    }


@router.post("/{job_id}/retry", response_model=JobResponse)
def retry_job(job_id: UUID, db: Session = Depends(get_db)):
    """Manually retry a DEAD or FAILED job.

    Resets retry_count so the job gets a full fresh retry budget — the
    operator is asserting the root cause is fixed, so prior attempts
    shouldn't count against it.
    """
    job = db.query(Job).filter(Job.id == job_id).first()

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in (JobStatus.PENDING, JobStatus.RUNNING):
        raise HTTPException(
            status_code=409,
            detail=f"Job is {job.status.value} — already queued or in progress",
        )

    if job.status == JobStatus.SUCCESS:
        raise HTTPException(
            status_code=409,
            detail="Job already succeeded — retrying would duplicate its side effects",
        )

    # Conditional transition: only from a terminal/retryable state.
    rows = (
        db.query(Job)
        .filter(
            Job.id == job_id,
            Job.status.in_([JobStatus.DEAD, JobStatus.FAILED]),
        )
        .update({
            Job.status: JobStatus.PENDING,
            Job.retry_count: 0,
            Job.next_retry_at: None,
            Job.error_message: None,
            Job.updated_at: datetime.now(timezone.utc),
        })
    )
    db.commit()

    if rows == 0:
        raise HTTPException(
            status_code=409,
            detail="Job status changed concurrently — please retry the request",
        )

    db.refresh(job)

    score = PRIORITY_SCORES[job.priority.value]
    redis_client.zadd(REDIS_QUEUE_KEY, {str(job.id): score})

    return job


@router.get("/status/{status}", response_model=list[JobResponse])
def list_jobs_by_status(
    status: JobStatus,
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """List jobs by status, most recent first. Primary use: inspecting DEAD jobs."""
    jobs = (
        db.query(Job)
        .filter(Job.status == status)
        .order_by(Job.updated_at.desc())
        .limit(limit)
        .all()
    )
    return jobs


@router.get("/stats/health")
def queue_health(db: Session = Depends(get_db)):
    """Compare Redis queue depth against PostgreSQL PENDING count.

    These should match when idle. Divergence indicates jobs committed to
    PostgreSQL whose Redis push was lost — the sweeper repairs these.
    """
    redis_depth = redis_client.zcard(REDIS_QUEUE_KEY)
    pending_count = db.query(Job).filter(Job.status == JobStatus.PENDING).count()

    return {
        "redis_queue_depth": redis_depth,
        "postgres_pending": pending_count,
        "divergence": pending_count - redis_depth,
        "healthy": pending_count == redis_depth,
    }