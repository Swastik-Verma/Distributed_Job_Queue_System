"""
Full-system integration test.

Submits a mixed workload that exercises every path simultaneously:
successes, transient failures that recover, transient failures that
exhaust retries, permanent failures, and all three priority tiers.

Run with API + workers + sweeper all running.
"""
import requests

API_URL = "http://localhost:8000/jobs"


def submit(job_type, priority, payload, max_retries=None):
    body = {"type": job_type, "payload": payload, "priority": priority}
    if max_retries is not None:
        body["max_retries"] = max_retries

    r = requests.post(API_URL, json=body)
    if r.status_code != 201:
        print(f"  FAILED to submit: {r.status_code} {r.text}")
        return None
    return r.json()["id"]


if __name__ == "__main__":
    submitted = {"clean": [], "flaky": [], "doomed": [], "permanent": []}

    # 1. Clean jobs across all three tiers — should all SUCCEED first try
    print("Submitting 12 clean jobs (all tiers)...")
    for i in range(6):
        submitted["clean"].append(submit("fraud_check", "high", {"txn": f"t{i}"}))
    for i in range(4):
        submitted["clean"].append(submit("generate_pdf", "medium", {"file": f"d{i}.pdf"}))
    for i in range(2):
        submitted["clean"].append(submit("cleanup_logs", "low", {"folder": f"/tmp/{i}"}))

    # 2. Flaky jobs — ~50% failure rate, should mostly SUCCEED after retries
    print("Submitting 8 flaky jobs...")
    for i in range(8):
        submitted["flaky"].append(submit("flaky_test", "medium", {"n": i}))

    # 3. Always-fail jobs — should exhaust retries and reach DEAD
    print("Submitting 3 doomed jobs...")
    for i in range(3):
        submitted["doomed"].append(submit("fail_test", "high", {"n": i}))

    # 4. Permanent failures — should reach DEAD on attempt 1, no retries
    print("Submitting 3 permanent-failure jobs...")
    submitted["permanent"].append(submit("no_such_handler", "high", {}))
    submitted["permanent"].append(submit("send_email", "medium", {}))       # missing 'to'
    submitted["permanent"].append(submit("generate_pdf", "low", {}))        # missing 'file'

    total = sum(len(v) for v in submitted.values())
    print(f"\n{total} jobs submitted.")
    print("Expected end state:")
    print("  12 clean      → SUCCESS, retry_count=0")
    print("   8 flaky      → mostly SUCCESS with retry_count>0, some DEAD")
    print("   3 doomed     → DEAD, retry_count=4")
    print("   3 permanent  → DEAD, retry_count=1")