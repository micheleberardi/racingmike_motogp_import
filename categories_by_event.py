import pymysql
import hashlib
import requests
import sys
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

querySelect = "SELECT * FROM racingmike_motogp.events WHERE year = '2025' AND test != 1 AND date_start BETWEEN CURDATE() - INTERVAL 50 DAY AND CURDATE() ORDER BY date_start DESC;"
cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    event_id = row['id']
    year = row['year']
    url = "https://api.motogp.pulselive.com/motogp/v1/results/categories?eventUuid="+str(event_id)

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        logging.error(f"Request failed for event {event_id}: {e}")
        continue

    if response.status_code != 200:
        logging.error(f"Failed to get data for event {event_id}, status code: {response.status_code}")
        continue

    data = response.json()
    logging.info(data)
    for d in data:
        category_id = d['id']
        legacy_id = d['legacy_id']
        name = d['name']

        md5= str(category_id)+str(legacy_id)+str(name)+str(year)
        md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()

        dict_mysql = {"id": category_id, "legacy_id": legacy_id, "name": name, "year": year, "md5": md5}
        # build column names and placeholders
        columns = []
        placeholders = []
        updates = []
        for key in dict_mysql.keys():
            columns.append(key)
            placeholders.append("%s")
            if key != "id":
                updates.append(f"{key}=VALUES({key})")

        # convert integer to string
        columns = [str(col) for col in columns]

        # join column names and placeholders into a string
        column_names = ", ".join(columns)
        value_placeholders = ", ".join(placeholders)
        update_clause = ", ".join(updates)

        # build insert statement
        try:
            query = f"INSERT INTO categories_by_event ({column_names}) VALUES ({value_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

            # execute insert statement with values
            values = tuple(dict_mysql.values())
            # logging.info(dict_mysql)
            logging.info(query)
            logging.info(values)
            cursor.execute(query, values)
        except Exception as e:
            logging.error(e)
    cnx.commit()
logging.info("DB connection closed")
cursor.close()
cnx.close()
