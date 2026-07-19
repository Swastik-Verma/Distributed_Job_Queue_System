from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Job, JobStatus
from app.schemas import JobCreate
from app.redis_client import redis_client

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("")
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
    queue_name = f"redis_queue:{new_job.priority.value}"
    redis_client.lpush(queue_name, str(new_job.id))

    return new_job