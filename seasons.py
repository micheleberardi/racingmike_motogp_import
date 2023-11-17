import pymysql
import requests
import hashlib

import os
from dotenv import load_dotenv

load_dotenv()

# Connect to the database
cnx = pymysql.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    passwd=os.getenv('DB_PASSWD'),
    db=os.getenv('DB_NAME'),
    cursorclass=pymysql.cursors.DictCursor,
    use_unicode=True,
    charset=os.getenv('DB_CHARSET')
)

cursor = cnx.cursor()



url = "https://api.motogp.pulselive.com/motogp/v1/results/seasons"
response = requests.get(url)
data = response.json()
print(data)
for d in data:
    id = d['id']
    name = d['name']
    year = d['year']
    current = d['current']
    md5 = str(id)+str(year)+str(current)
    md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
    try:
        insertquery = "INSERT INTO seasons (id, name, year, current,md5) VALUES (%s,%s, %s, %s,%s)"
        insertvalues = (id, name, year, current,md5)
        print(insertquery)
        cursor.execute(insertquery, insertvalues)
        cnx.commit()
    except Exception as e:
        print(e)
    #cursor.close()
    #cnx.close()