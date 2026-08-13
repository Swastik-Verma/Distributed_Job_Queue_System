from pydantic import BaseModel, field_validator
from typing import Any, Dict, Optional
from app.models import JobPriority, JobStatus

from datetime import datetime
from uuid import UUID

class JobCreate(BaseModel):
    type: str
    priority: Optional[JobPriority] = JobPriority.MEDIUM
    payload: Dict[str, Any]
    max_retries: int = 3

    @field_validator("priority", mode="before")
    @classmethod
    def normalize_priority(cls,v):
        if isinstance(v, str):
            return v.upper()
        return v


class JobResponse(BaseModel):
    id: UUID
    type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: JobPriority
    retry_count: int
    max_retries: int
    next_retry_at: Optional[datetime]
    error_message: Optional[str]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

