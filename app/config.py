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