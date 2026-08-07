from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

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