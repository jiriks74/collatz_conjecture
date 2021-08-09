import mysql.connector
from decouple import config

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
        ;param commit: bool
        ;return: None
        """
        self.cursor.execute(query, val or ()) # Execute query

        if commit: # Commit to database if passed so
            self.database.commit()

class data:
    """
    Class for working with numbers in database
    """
    def tablename(self, number) -> str:
        """
        Get table name for a number
        ;param number: int
        ;return: str
        """
        return f"{number // 1000000 + 1}m"

    def write(self, number, next_number, thread) -> None:
        """
        Write working number and next_number into database
        ;param number: int
        ;param next_number: int
        ;param thread: int
        ;return: None
        """   
        with MySQLWrapper() as db:
            db.execute(f"INSERT INTO `{self.tablename(number)}` (`number`, `next_number`, `421_loop`, `thread`) VALUES ('{number}', '{next_number}', '0', '{thread}');") 

    def check(self, number) -> bool:
        """
        Check if number exists in database and return if it falls into 421 loop
        ;return: bool
        """
        with MySQLWrapper() as db:
            tabname = self.tablename(number)
            while True: # Loop so I can check the values repeatedly when other thread is working on them

                table = db.query(f"SELECT `421_loop`, `thread` FROM `{tabname}` WHERE `number`='{number}';", cache=False)

                if table[0][1] == 0: # Check if other thread is working with this number (all data should be on first row) (other than None means thread is working on it)
                    
                    if table[0][0] == 1: # If true return true
                        return True

                    elif table[0][0] == 0: # If false return false
                        return False

                    else: # Else - nonsense value has appeared in database - WTF? JustInCase
                        print(f"""Nonsense in database was found:
                        In table: {tablenum}
                        At number: {number}""")

                        exit()

    def set_loop(self, start_number, thread) -> None:
        """
        Set nuber sequence in database to 421_loop: true
        ;param start_number: int
        ;param thread: int
        ;return: None
        """
        with MySQLWrapper() as db:
            acnum = start_number # Active number
            while True: # Used to iterate through numbers in database
                tabname = self.tablename(acnum) # Get the table of the number
                table = db.query(f"SELECT `number`, `next_number`, `421_loop`, `thread` FROM `{tabname}` WHERE `number`='{acnum}';") # Get all the data about the number

                if table[0][2] == 0: # Check if 421_loop is set to false (othervise stop)
                    if table[0][3] == thread: # If the thread number is right
                        db.execute(f"UPDATE `{tabname}` SET `421_loop`='1', `thread`='0' WHERE `number`='{acnum}';", commit=False) # Set thread to 0 (as no thread is working on this number) and update 421_loop to true
                        acnum = table[0][1] # Get next number

                    else: 
                        print(f"""Nonsense occured - looks like a thread is working with number that it shouldn't be:
                        Table: {tabname}
                        Number: {acnum}
                        No data was committed""")
                        exit()

                else:
                    break
        
            db.execute("", commit=True)



if __name__ == "__main__":
    print(data().check(1))