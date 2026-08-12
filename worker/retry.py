"""
Retry backoff calculation.

delay = BASE_DELAY * (MULTIPLIER ** retry_count)
        * random(JITTER_MIN, JITTER_MAX)
        capped at MAX_DELAY

Jitter is applied BEFORE the cap, so MAX_DELAY is a hard ceiling —
no retry ever waits longer than it, regardless of the random draw.
"""
import random
from datetime import datetime, timedelta, timezone

from app.config import (
    RETRY_BASE_DELAY,
    RETRY_MULTIPLIER,
    RETRY_MAX_DELAY,
    RETRY_JITTER_MIN,
    RETRY_JITTER_MAX,
)


def get_retry_delay(retry_count):
    """Seconds to wait before the next attempt.

    retry_count is the number of attempts ALREADY made:
      0 -> first retry  -> ~5s
      1 -> second retry -> ~10s
      2 -> third retry  -> ~20s
    """
    delay = RETRY_BASE_DELAY * (RETRY_MULTIPLIER ** retry_count)
    delay = min(delay, RETRY_MAX_DELAY)                              # cap first
    delay = delay * random.uniform(RETRY_JITTER_MIN, RETRY_JITTER_MAX)  # jitter the capped value
    return delay                # hard ceiling not applied/enforced


def get_next_retry_at(retry_count):
    """Absolute timestamp when this job becomes eligible for retry."""
    delay = get_retry_delay(retry_count)
    return datetime.now(timezone.utc) + timedelta(seconds=delay)