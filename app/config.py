# Redis key name — one sorted set replaces three lists
REDIS_QUEUE_KEY = "job_queue"

# Priority-to-score mapping for the sorted set
# Lower score = popped first = higher priority
# Gaps (1/5/10 instead of 1/2/3) leave room for future priority tiers
PRIORITY_SCORES = {
    "HIGH": 1,
    "MEDIUM": 5,
    "LOW": 10,
}


# Weighted round-robin cycle for priority selection.
# Controls what SHARE of worker capacity each tier receives when
# multiple tiers have jobs waiting. Tunable without touching worker logic.
PRIORITY_WEIGHTS = {
    "HIGH": 6,
    "MEDIUM": 3,
    "LOW": 1,
}

# Expanded into the actual cycle the worker walks
WEIGHT_CYCLE = [
    tier
    for tier, weight in PRIORITY_WEIGHTS.items()
    for _ in range(weight)
]