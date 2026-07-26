import os
from multiprocessing import Process

counter = 0

def increment():
    global counter
    counter += 1
    print(f" child PID {os.getpid()}: counter = {counter}")


if __name__ == "__main__":
    print(f"parent PID {os.getpid()}: counter = {counter} (before)")
    
    processes = [Process(target=increment) for _ in range(3)]
    for p in processes:
        p.start()
    for p in processes:
        p.join()

    print(f"parent PID {os.getpid()}: counter = {counter} (after)")
