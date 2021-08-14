from dal import data
from datetime import datetime
from queue import Queue
import threading
from multiprocessing import cpu_count
from os import name, system
from time import sleep
class compute:
    def __init__(self) -> None:
        pass

    def main_thread(self) -> None:
        """
        Function that the main thread is running. Program should be started with this function (__init__ breaks some functionality)
        ;return: None
        """
        self.clear() # Clear the terminal
        
        self.q = Queue() # Initialize queue with 2 entries - for communication between threads
        self.q.put(0) # First entry if to singalize that the program is stopping and every thread should not start new calculations
        self.q.put(0) # For start numbers so the threads won't calculate the same number twice (can create duplicate error in database - ask @jiriks74)

        try:
            threads = [] # List of threads
            for i in range(cpu_count()-1): # Creates list of threads that has number of threads in the system while leaving one free, so the os would have at least one free
                threads.append(threading.Thread(target=self.solve, args=())) # Create new thread that runs the solve function
                threads[i].start() # Start the new thread
                print(f"{threads[i].getName()} was started")
                sleep(0.5) # Wait a while

            print(f"All threads started at: {datetime.now()}")

            while True: pass # So the main thread won't exit the program

        except KeyboardInterrupt: # To stop the program gracefully
            print("Please wait, the program can take a long time to stop. All computations have to by completed for proper shutdown.")

            self.q.queue[0] = "stop" # Set exit flag in queue

            for thread in threads: # Wait until all threads close
                while thread.isAlive(): pass

                exit() # Exit

    def solve(self) -> None:
        """
        Contrlols threads so they are solving without colliding with eacht other
        ;return: None
        """
        while True:
            acnum, self.q.queue[1] = self.q.queue[1] + 1 # Get next number I can work on and save it so no other thread is working on it

            if self.q.queue[0] != "stop": # Chect if the program should stop
                self.compute(acnum) # Calculate the conjecture for a number

            else:
                print(f"Thread '{threading.currentThread().getName()} stopped.")
                break

    def compute(self, startnum) -> None:
        """
        Function for computing if passed number falls into 421_loop and writing result to the database
        ;param startnum (number that you want to calculate the sequence for)
        ;return None
        """
        try:
            acnum = startnum 
            dt = data()

            while True:
                exists, loop = dt.check(acnum) # Check if number exists and if it is, is it in 421_loop
                if not exists: # Calculate next number
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
                        print(f"""Non 421 number found!:
                        Startnum: {startnum}
                        Table: {dt.tablename(startnum)}""")
                        exit()
        
        except Exception as e: # Try to stop the program in a way that database stays as clean as possible
            self.q.queue[0] = "stop" # Set program stop flag
            print(e)
        
    def clear(self):
        """
        Clears the console both on Windows and Linux.
        
        Takes no input
        """
        command = 'clear'
        if name in ('nt', 'dos'):  # If Machine is running on Windows, use cls
            command = 'cls'
        system(command)


if __name__ == "__main__":
    compute().main_thread()