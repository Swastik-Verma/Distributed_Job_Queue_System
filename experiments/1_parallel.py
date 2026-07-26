import time
from multiprocessing import Process

def do_work(name):
    print(f"{name} starting")
    time.sleep(2)          # pretend this is sending an email
    print(f"{name} finished")


if __name__ == "__main__":
    # --- Sequential ---
    start = time.time()
    do_work("A")
    do_work("B")
    do_work("C")
    print(f"Sequential took {time.time() - start:.1f}s\n")


    # --- Parallel ---
    start = time.time()
    processes = [Process(target=do_work, args=(n,)) for n in ["A", "B", "C"]]

    for p in processes:
        p.start()          # fork a new OS process, run do_work in it

    for p in processes:
        p.join()           # wait here until that process exits

    print(f"Parallel took {time.time() - start:.1f}s")