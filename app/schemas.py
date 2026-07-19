from pydantic import BaseModel
from typing import Any, Dict, Optional
from app.models import JobPriority

class JobCreate(BaseModel):
    type: str
    priority: Optional[JobPriority] = JobPriority.MEDIUM
    payload: Dict[str, Any]