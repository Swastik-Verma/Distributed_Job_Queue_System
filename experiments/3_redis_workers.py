import os
import time
from multiprocessing import Process

import redis

def worker(worker_id):
    # NOTE: connection created *inside* the process, not passed in
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)


    while True:
        task = r.rpop("practice_queue")

        if task is None:
            print(f"[worker {worker_id}] queue empty, exiting")
            break

        print(f"[worker {worker_id}] PID {os.getpid()} processing {task}")
        time.sleep(1)          # pretend the task takes a second


if __name__ == "__main__":
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    # seed the queue with 9 fake tasks
    r.delete("practice_queue")
    for i in range(1,10):
        r.lpush("practice_queue", f"task-{i}")

    start = time.time()

    processes = [Process(target=worker, args=(i,)) for i in [1,2,3]]
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    print(f"\n9 tasks, 3 workers, {time.time() - start:.1f}s")