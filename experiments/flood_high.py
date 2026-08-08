"""
Stress test: flood the queue with HIGH priority jobs continuously
to verify that LOW jobs are not starved.
"""
import time
import requests

API_URL = "http://localhost:8000/jobs"

TOTAL_HIGH_JOBS = 40
DELAY_BETWEEN_SUBMITS = 0.5  # seconds


def submit_job(job_type, priority, payload):
    response = requests.post(API_URL, json={
        "type": job_type,
        "payload": payload,
        "priority": priority,
    })
    if response.status_code != 201:
        print(f"  submit failed: {response.status_code} {response.text}")
        return None
    return response.json()["id"]


if __name__ == "__main__":
    print("Submitting 3 LOW jobs first...")
    low_ids = []
    for i in range(3):
        job_id = submit_job("cleanup_logs", "low", {"batch": i})
        low_ids.append(job_id)
        print(f"  LOW job {i+1}: {job_id}")

    print(f"\nSubmitting {TOTAL_HIGH_JOBS} HIGH jobs (no delay)...")
    for i in range(TOTAL_HIGH_JOBS):
        submit_job("fraud_check", "high", {"txn": f"txn_{i}"})
        print(f"  HIGH job {i+1}/{TOTAL_HIGH_JOBS} submitted")
        # no time.sleep here — submit as fast as possible

    print("\nAll jobs submitted. LOW job IDs to watch:")
    for job_id in low_ids:
        print(f"  {job_id}")
    print("\nNow start the workers.")