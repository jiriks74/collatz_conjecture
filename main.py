import mysql.connector
from decouple import config

db = mysql.connector.connect(host="10.243.12.5",
    port=int(config('port')),
    user=config('user'),
    passwd=config('passwd'),
    db=config('db'),
    ssl_ca=config('ssl_ca'),
    ssl_key=config('ssl_key'),
    ssl_cert=config('ssl_cert')
)

cursor = db.cursor()

number = 1
table = f"{number // 1000000 + 1}m"

#cursor.execute(f"SELECT `421_loop`, `thread` FROM `{table}` WHERE `number`='{number}'")
cursor.execute("SELECT * FROM `1m`")

row = cursor.fetchone()

print(f"{row[0]}, {row[1]}.")

print(db)