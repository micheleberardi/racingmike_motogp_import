import os
import pymysql
import json
from dotenv import load_dotenv

# Load environment variables
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

try:
    # Prepare a cursor object using cursor() method
    cursor = cnx.cursor()

    # SQL query to execute
    #querySelect = "SELECT cg.*, e.*, s.*, r.*, rec.*, se.* FROM events e LEFT JOIN categories_general cg ON e.year = cg.year LEFT JOIN sessions s ON e.year = s.year LEFT JOIN results r ON e.year = r.year LEFT JOIN records rec ON e.year = rec.year LEFT JOIN seasons se ON e.year = se.year WHERE e.year = 2023 and e.id = 'a08837b6-1cfb-4dfe-a7a4-d61fe970ea3d';"
    querySelect = "SELECT cg.*, e.*, s.*, r.*, rec.*, se.* FROM events e LEFT JOIN categories_general cg ON e.year = cg.year LEFT JOIN sessions s ON e.year = s.year AND e.id = s.event_id LEFT JOIN results r ON e.year = r.year AND e.id = r.event_id LEFT JOIN records rec ON e.year = rec.year LEFT JOIN seasons se ON e.year = se.year WHERE e.year = 2023 AND e.id = 'a08837b6-1cfb-4dfe-a7a4-d61fe970ea3d';"
    # Execute the SQL command
    cursor.execute(querySelect)

    # Fetch all the rows in a list of lists.
    result = cursor.fetchall()
    print(result)

    # Write result to a JSON file
    with open('result.json', 'w') as json_file:
        json.dump(result, json_file, indent=4, default=str)

    print("Data saved to result.json")

finally:
    # disconnect from server
    cnx.close()
