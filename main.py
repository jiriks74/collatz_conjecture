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

cursor.execute("SELECT * FROM `1m`")

print(cursor.fetchall())

print(db)