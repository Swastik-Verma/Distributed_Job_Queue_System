"""
Worker exception types.

PermanentFailure signals a failure that retrying cannot fix — the input
or the system is wrong, not temporarily unavailable. Any other exception
is treated as transient and follows the normal backoff/retry path.
"""


class PermanentFailure(Exception):
    """Raise when a job can never succeed with the same input.

    Examples: unknown job type, missing required payload field,
    malformed data. NOT for timeouts, connection errors, or rate limits —
    those are transient and should be retried.
    """
    pass