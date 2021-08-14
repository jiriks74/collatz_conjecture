from dal import data
import threading
from queue import Queue
import multiprocessing
import os

import time
from datetime import datetime
from random import random
class compute:
    def __init__(self) -> None:
        self.q = Queue()
        self.q.put(0)
        self.q.put(1)

    def main_thread(self) -> None:
        threads = []
        for i in range(multiprocessing.cpu_count()-1):
        #for i in range(1):
            threads.append(threading.Thread(target=self.solve, args=()))
            threads[i].start()
            print(f"{threads[i].getName()} was started")
            time.sleep(1)

        while True:
            
            if input("Insert q to quit the program: ") == "q":
                print("Please wait, the program can take a long time to stop. All computations have to by completed for proper shutdown.")
                self.q.queue[0] = "stop"

                for thread in threads:
                    while thread.isAlive():
                        pass

                    exit()

            else:
                self.clear()

    def solve(self):
        while True:
            acnum = self.q.queue[1]
            #print (self.q.queue[0])
            if self.q.queue[0] != "stop":
                self.compute(acnum)
                #self.periodic_messages(1)
                self.q.queue[1] += 1

            else:
                print(f"Thread '{threading.currentThread().getName()} stopped.")
                break

    def compute(self, startnum) -> None:
        """
        Function for computing if passed number falls into 421_loop and writing result to the database
        ;param startnum (number that you want to calculate the sequence for)
        ;return None
        """
        acnum = startnum
        dt = data()
        while True:
            exists, loop = dt.check(acnum)
            if not exists:
                if acnum % 2 == 0:
                    next_num = acnum / 2
                    dt.write(acnum, next_num)
                    acnum = next_num

                else:
                    next_num = 3 * acnum + 1
                    dt.write(acnum, next_num)
                    acnum = next_num
            else:
                if loop:
                    dt.set_loop(startnum)
                    break
                if not loop:
#                   dt.set_loop(startnum, threading.currentThread().getName())
                    print(f"""Non 421 number found!:
                    Startnum: {startnum}
                    Table: {dt.tablename(startnum)}""")
                    exit()

    def clear(self):
        """
        Clears the console both on Windows and Linux.
        
        Takes no input
        """
        command = 'clear'
        if os.name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
            command = 'cls'
        os.system(command)


if __name__ == "__main__":
    compute().main_thread()