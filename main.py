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

            self.q.queue[1] = f"{thread_name}: {datetime.now()}"
            self.q.queue[0] = 1
            #print(f"Message put into q and flag was set.\n The message was: \'{q.get()}\'")
            time.sleep(delay)

    def compute(self) -> None:
        acnum = 1
        dt = data()
        while True:
            if dt.check(acnum):
                if acnum % 2 == 0:
                    pass
                else:
                    pass
            else:
                print(f"""Probably error:
                Number {acnum} doesn't fall into 421_loop
                """)
                exit()

if __name__ == "__main__":
    compute().start_threads()