from logging import exception
import threading
import mysql.connector
from decouple import config
from mysql.connector.errors import ProgrammingError

class MySQLWrapper:
    """
    Small wrapper for mysql.connector, so I can use magic with statement. Because readibility counts!
    """

    def __init__(self, **credentials):
        if not credentials:
            self.credentials = {"host": config('host'), "port": config('port'), "user": config('user'), "passwd": config('passwd'), "db": config('db'),
            "ssl_ca": config('ssl_ca'), "ssl_key": config('ssl_key'), "ssl_cert": config('ssl_cert')}
        else:
            self.credentials = credentials

    def __enter__(self):
        self.database = mysql.connector.connect(**self.credentials)
        self.cursor = self.database.cursor()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        try:
            self.cursor.close()
            self.database.close()

        except AttributeError:
            print('Not closable.')
            return True

    def query(self, query: str, val=None, cache=True):
        """
        Query of database. Returns list tuples from database.
        ;param query: str
        ;param val: Optional
        ;param chache: bool (optional, True by defaut)
        ;return: list of tuples
        """
        self.cursor.execute(query, val or ()) # Execute query

        response = self.cursor.fetchall() # Fetch the data

        if not cache: # Hack to avoid caching - if I commit the cache will be cleared
            self.database.commit()

        return response # Return the response

    def execute(self, query, val=None, commit=True):
        """
        Execute your values and commit them. Or not. Your decision.
        ;param query: str
        ;param val: Optional
        ;param commit: bool (optional, True by default)
        ;return: None
        """
        self.cursor.execute(query, val or ()) # Execute query

        if commit: # Commit to database if passed so
            self.database.commit()

class data:
    """
    Class for working with numbers in database
    """

    def __init__(self, **credentials) -> None:
        if credentials:
            self.credentials = credentials

    def tablename(self, number) -> str:
        """
        Get table name for a number
        ;param number: int
        ;return: str
        """
        return f"{int(number // 1000000 + 1)}m"

    def write(self, number, next_number) -> None:
        """
        Write working number and next_number into database
        Sets 421_loop to False by default, and uses current thread name
        ;param number: int
        ;param next_number: int
        ;return: None
        """   
        thread = threading.currentThread().getName()
        tablename = self.tablename(number)
        with MySQLWrapper() as db:
            db.execute(f"""CREATE TABLE IF NOT EXISTS `{tablename}` (
                `number` BIGINT UNSIGNED NOT NULL PRIMARY KEY,
                `next_number` BIGINT UNSIGNED NOT NULL,
                `421_loop` BOOLEAN NOT NULL,
                `thread` CHAR(107) NOT NULL
                );""")

            db.execute(f"UPDATE `{tablename}` SET `number` = '{number}', `next_number` = '{next_number}', `421_loop` = '0', `thread` = '{thread}' WHERE `number` = '{number}';") 
            
    def check(self, number):
        """
        Check if number exists in database and return if it falls into 421 loop.
        If another number is working with passed number, this function will wait, until the other thread is done.
        If this number is working with this number, this function will return 421_loop as False, as the number exists, but the thread shouldn't come to one number twice if this math problem is true.
        ;param: number
        ;return: bool (number exists)
        ;return: bool (421_loop)
        """
        thread = threading.currentThread().getName()

        with MySQLWrapper() as db:
            tabname = self.tablename(number)
            while True: # Loop so I can check the values repeatedly when other thread is working on them

                try:
                    table = db.query(f"SELECT `421_loop`, `thread` FROM `{tabname}` WHERE `number`='{number}';", cache=False)
                    
                except ProgrammingError as e:
                    if str(e).startswith("1146"):
                        return False, False

                    elif str(e).startswith("1062"):
                        print("duplicate")

                    else:
                        from main import compute
                        compute().q.queue[0] = "stop"
                        print(e)

                #print(table)
                if len(table) == 0:
                    db.execute(f"INSERT INTO `{tabname}` (`number`, `next_number`, `421_loop`, `thread`) VALUES ('{number}', '{0}', '0', '{thread}');")
                    return False, False

                else:
                    if table[0][1] == '0': # Check if other thread is working with this number (all data should be on first row) (other than None means thread is working on it)
                        
                        if table[0][0] == 1: # If 421 is true return true
                            return True, True

                        elif table[0][0] == 0: # If 421 is false return false
                            return True, False

                        else: # Else - nonsense value has appeared in database - WTF? JustInCase
                            print(f"""Nonsense in database was found:
                            In table: {tabname}
                            At number: {number}""")

                            exit()
                    elif table[0][1] == thread: # If the current thread created this number, loop other than 421 has been found: number exists, but it isn't 421
                        return True, False

    def set_loop(self, start_number) -> None:
        """
        Set nuber sequence in database to 421_loop: true
        ;param start_number: int
        ;return: None
        """
        thread = threading.currentThread().getName()

        with MySQLWrapper() as db:
            acnum = start_number # Active number
            while True: # Used to iterate through numbers in database
                tabname = self.tablename(acnum) # Get the table of the number
                table = db.query(f"SELECT `number`, `next_number`, `421_loop`, `thread` FROM `{tabname}` WHERE `number`='{acnum}';") # Get all the data about the number

                if table[0][3] == thread: # Check if number belongs to current thread
                    if table[0][2] == 0: # Check if loop is already set
                        db.execute(f"UPDATE `{tabname}` SET `421_loop`='1', `thread`='0' WHERE `number`='{acnum}';", commit=False) # Set thread to 0 (as no thread is working on this number) and update 421_loop to true
                        acnum = table[0][1] # Get next number

                    else: 
                        break

                else:
                    break
        
            db.execute("", commit=True)



if __name__ == "__main__":
    if False: print("yup")
    print(data().check(8))
    print(data().write(8, 4))
    print(data().check(8))
    print(data().set_loop(8))