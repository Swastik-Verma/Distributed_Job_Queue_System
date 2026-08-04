import os
from multiprocessing import Process
from worker.worker import run_worker

WORKER_COUNT = 3


def main():
    print(f"[launcher] starting {WORKER_COUNT} workers (parent PID {os.getpid()})")

    processes = []
    for i in range(WORKER_COUNT):
        p = Process(target=run_worker, name=f"worker-{i+1}")
        processes.append(p)
        p.start()
        print(f"[launcher] started worker-{i+1} (PID {p.pid})")

    print(f"[launcher] all {WORKER_COUNT} workers running\n")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print(f"\n[launcher] shutting down...")
        for p in processes:
            p.terminate()
            p.join()
        print(f"[launcher] all workers stopped")


if __name__ == "__main__":
    main()