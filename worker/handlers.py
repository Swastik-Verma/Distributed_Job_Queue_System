"""
Job handlers — the actual work each job type performs.

Registry pattern: job.type is looked up in HANDLERS to find the
function that processes it. Adding a new job type means writing a
function and registering it here — no changes to worker loop logic.
"""
import time
from worker.exceptions import PermanentFailure


def handle_send_email(payload):
    """Simulated email send."""
    to = payload.get("to")
    if not to:
        raise PermanentFailure("send_email requires a 'to' field in payload")
    print(f"    → sending email to {to}")
    time.sleep(2)
    return {"sent_to": to}


def handle_generate_pdf(payload):
    """Simulated PDF generation."""
    filename = payload.get("file")
    if not filename:
        raise PermanentFailure("generate_pdf requires a 'file' field in payload")
    print(f"    → generating PDF: {filename}")
    time.sleep(3)
    return {"generated": filename}


def handle_fraud_check(payload):
    """Simulated fraud check."""
    txn = payload.get("txn") or payload.get("transaction_id")
    if not txn:
        raise PermanentFailure("fraud_check requires a 'txn' field in payload")
    print(f"    → running fraud check on {txn}")
    time.sleep(1)
    return {"checked": txn, "verdict": "clean"}


def handle_cleanup_logs(payload):
    """Simulated log cleanup. Naturally idempotent — deleting
    already-deleted files is harmless."""
    folder = payload.get("folder", "/tmp/logs")
    print(f"    → cleaning logs in {folder}")
    time.sleep(2)
    return {"cleaned": folder}


def handle_fail_test(payload):
    """Deliberate failure trapdoor. Kept permanently for retry/DLQ
    testing in Week 5."""
    raise Exception("Simulated failure: email server unreachable")


def handle_flaky_test(payload):
    """Fails roughly 50% of the time. For testing that retry
    eventually succeeds rather than always exhausting max_retries."""
    import random
    if random.random() < 0.5:
        raise Exception("Simulated transient failure: connection reset")
    print("    → flaky job succeeded this time")
    time.sleep(1)
    return {"result": "ok"}


HANDLERS = {
    "send_email": handle_send_email,
    "generate_pdf": handle_generate_pdf,
    "fraud_check": handle_fraud_check,
    "cleanup_logs": handle_cleanup_logs,
    "fail_test": handle_fail_test,
    "flaky_test": handle_flaky_test,
}


