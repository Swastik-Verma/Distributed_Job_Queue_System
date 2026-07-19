from fastapi import FastAPI

from app.database import init_db
from app.routers import jobs

app = FastAPI(title="Distributed Job Queue System")

@app.on_event("startup")
def startup():
    init_db()

app.include_router(jobs.router)

@app.get("/")
def read_root():
    return {"message": "Job Queue System is alive!"}

@app.get("/health")
def health_check():
    return {"status": "ok"}

