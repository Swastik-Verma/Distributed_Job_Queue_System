"""
Balanced load test: submit jobs across all three tiers in quantities
large enough that no tier runs dry, so the actual 6:3:1 throughput
ratio becomes measurable.
"""
import requests

API_URL = "http://localhost:8000/jobs"

# Enough of each tier that none runs dry before the others
HIGH_COUNT = 60
MEDIUM_COUNT = 30
LOW_COUNT = 30


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
    print(f"Submitting {HIGH_COUNT} HIGH jobs...")
    for i in range(HIGH_COUNT):
        submit_job("fraud_check", "high", {"txn": f"high_{i}"})

    print(f"Submitting {MEDIUM_COUNT} MEDIUM jobs...")
    for i in range(MEDIUM_COUNT):
        submit_job("generate_pdf", "medium", {"file": f"doc_{i}.pdf"})

    print(f"Submitting {LOW_COUNT} LOW jobs...")
    for i in range(LOW_COUNT):
        submit_job("cleanup_logs", "low", {"batch": i})

    total = HIGH_COUNT + MEDIUM_COUNT + LOW_COUNT
    print(f"\nDone — {total} jobs submitted. Verify with: redis-cli ZCARD job_queue")
    print("Then start the workers.")