# from sqlalchemy import Column, Integer, String
# from database import Base

# class Item(Base):
#     __tablename__ = "items"

#     id = Column(Integer, primary_key=True, index=True)
#     name = Column(String, index=True)


import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Enum, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
import enum
from app.database import Base

# --- Enums ---

class JobStatus(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    DEAD = "DEAD"

class JobPriority(enum.Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

# --- Job Table ---

class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(String, nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    priority = Column(Enum(JobPriority), default=JobPriority.MEDIUM, nullable=False)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    next_retry_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)