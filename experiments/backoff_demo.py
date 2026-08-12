import random

BASE_DELAY = 5
MULTIPLIER = 2
MAX_DELAY = 300


def plain(retry_count):
    return min(BASE_DELAY * (MULTIPLIER ** retry_count), MAX_DELAY)


def with_jitter(retry_count):
    delay = plain(retry_count)
    return round(delay * random.uniform(0.5, 1.5), 2)


print(f"{'attempt':<9}{'no jitter':<12}{'3 jittered samples'}")
for attempt in range(8):
    samples = [with_jitter(attempt) for _ in range(3)]
    print(f"{attempt:<9}{plain(attempt):<12}{samples}")