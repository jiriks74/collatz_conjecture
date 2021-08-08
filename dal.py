import mysql.connector
from time import sleep
from decouple import config

class database:
    def __init__(self):
        try:
            self.db = mysql.connector.connect(host="10.243.12.5",
                port=int(config('port')),
                user=config('user'),
                passwd=config('passwd'),
                db=config('db'),
                ssl_ca=config('ssl_ca'),
                ssl_key=config('ssl_key'),
                ssl_cert=config('ssl_cert')
            )

            self.cursor = self.db.cursor()
        except mysql.connector.Error as e:
            print(e)
            print("=====================================")
            print("Failed to initialize connection with database.")

            exit()

    def write(self, number, next_number, thread):
        pass    

    def check(self, number):
        self.db.close()
        while True:
            self.__init__()
            table = f"{number // 1000000 + 1}m"

            self.cursor.execute(f"SELECT `421_loop`, `thread` FROM `{table}` WHERE `number`='{number}';")

            row = self.cursor.fetchall()
            self.db.close()

            print(row)

            if row[0][1] == 0:
                if row[0][0] == 1:
                    return True

                elif row[0][0] == 0:
                    return False

                """
            else:
                print(f"Fatal error:\n`421_loop` column had value of {row[0]} at number {number} in table {table}.\nWTF!?")
                """

    def set_loop(self, start_number):
        pass

if __name__ == "__main__":
    print(database().check(1))