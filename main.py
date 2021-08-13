from dal import data
import threading
from queue import Queue
import multiprocessing

import time
from datetime import datetime
class compute:
    def __init__(self) -> None:
        self.q = Queue()
        self.q.put("0")
        self.q.put("")

    def start_threads(self) -> None:
        threads = []
        for i in range(multiprocessing.cpu_count()-1):
            threads.append(threading.Thread(target=self.periodic_messages, args=(f"t{i}", i+5)))
            threads[i].start()
            print(f"{threads[i].getName()} was started")

        while True:
            if self.q.queue[0] == 1:
                print(self.q.queue[1])

                self.q.queue[0] = 0

    def periodic_messages(self, thread_name, delay):
        while True:
            while self.q.queue[0] == 1:
                pass

            self.q.queue[1] = f"{threading.currentThread().getName()}: {datetime.now()}"
            self.q.queue[0] = 1
            #print(f"Message put into q and flag was set.\n The message was: \'{q.get()}\'")
            time.sleep(delay)

    def compute(self, startnum) -> None:
        acnum = startnum
        dt = data()
        while True:
            exists, loop = dt.check(acnum)
            if not exists:
                if acnum % 2 == 0:
                    next_num = acnum / 2
                    dt.write(acnum, next_num, False, threading.currentThread().getName())
                    acnum = next_num

                else:
                    next_num = 3 * acnum + 1
                    dt.write(acnum, next_num, False, threading.currentThread().getName())
                    acnum = next_num
            else:
                if loop:
                    dt.set_loop(startnum, threading.currentThread().getName())
                    break
                if not loop:
                    dt.set_loop(startnum, threading.currentThread().getName())
                    print(f"""Non 421 number found!:
                    Startnum: {startnum}
                    Table: {startnum // 1000000 +1}m""")
                    exit()


if __name__ == "__main__":
    compute().start_threads()