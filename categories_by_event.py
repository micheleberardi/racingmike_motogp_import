import pymysql
import hashlib
import requests
import sys
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

querySelect = "SELECT * FROM events"
cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    id = row['id']
    year = row['season_year']
    url = "https://api.motogp.pulselive.com/motogp/v1/results/categories?eventUuid="+str(id)

    response = requests.get(url)
    data = response.json()
    print(data)
    for d in data:
        id = d['id']
        legacy_id = d['legacy_id']
        name = d['name']

        md5= str(id)+str(legacy_id)+str(name)+str(year)
        md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()

        dict_mysql = {"id": id, "legacy_id": legacy_id, "name": name, "year": year, "md5": md5}
        # build column names and placeholders
        columns = []
        placeholders = []
        for key in dict_mysql.keys():
            columns.append(key)
            placeholders.append("%s")

        # convert integer to string
        columns = [str(col) for col in columns]

        # join column names and placeholders into a string
        column_names = ", ".join(columns)
        value_placeholders = ", ".join(placeholders)

        # build insert statement

        try:
            query = f"INSERT INTO categories_by_event ({column_names}) VALUES ({value_placeholders})"

            # execute insert statement with values
            values = tuple(dict_mysql.values())
            # print(dict_mysql)
            print(query)
            print(values)
            cursor.execute(query, values)
            cnx.commit()
            print("***********************************")
        except Exception as e:
            print(e)


