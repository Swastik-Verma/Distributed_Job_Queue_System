# Experiments

Learning scripts and load tests. Not part of the running system.

- `flood_high.py` — deliberately imbalanced load (40 HIGH, 3 LOW) to
  test whether LOW jobs starve under sustained HIGH pressure.
- `balanced_load.py` — balanced load (60/30/30) so all tiers have
  demand, making the configured 6:3:1 throughput ratio measurable.
- Day 15 multiprocessing scripts — memory isolation, parallel timing.