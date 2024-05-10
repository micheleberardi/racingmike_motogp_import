import pymysql
import hashlib
import requests
import json
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

#querySelect = "SELECT * FROM events"
querySelect = "SELECT * FROM racingmike_motogp.events WHERE year = '2024' AND test = 1 AND date_start BETWEEN CURDATE() - INTERVAL 150 DAY AND CURDATE() ORDER BY date_start DESC;"
cursor.execute(querySelect)
result = cursor.fetchall()
#print(result)
for row in result:
    event_id = row['id']
    year = row['year']
    print("RUNNING YEAR "+str(year))
    querySelect3 = "SELECT * FROM racingmike_motogp.categories_general WHERE year = "+str(year)
    #querySelect3 = "SELECT * FROM sessions where year = 2024 AND date BETWEEN CURDATE() - INTERVAL 40 DAY AND CURDATE() ORDER BY date DESC"
    print(querySelect3)
    cursor.execute(querySelect3)
    result = cursor.fetchall()
    #print(result)
    for row in result:
        category_id = row['id']
        name = row['name']
        year = row['year']
        url = "https://api.motogp.pulselive.com/motogp/v1/results/sessions?eventUuid="+str(event_id)+"&categoryUuid="+str(category_id)
        print(url)
        print("***********************************")
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(data)
            # Assuming the JSON response is a list
            for session in data:
                # Constructing the INSERT query using the fetched data
                print(type(session['session_files']))  # This should show <class 'dict'> if it's correctly structured
                print(session['session_files'])  # This will show the actual content
                insert_query = """
                INSERT INTO sessions (
    id, date, number, track_condition, air_condition, humidity_condition, ground_condition,
    weather_condition, circuit_name, classification_url, analysis_url, average_speed_url,
    fast_lap_sequence_url, lap_chart_url, analysis_by_lap_url, fast_lap_rider_url, grid_url,
    session_url, world_standing_url, best_partial_time_url, maximum_speed_url,
    combined_practice_url, combined_classification_url, type, category_id,
    category_legacy_id, category_name, event_id, event_name, event_sponsored_name, year,
    circuit_id, circuit_legacy_id, circuit_place, circuit_nation, country_iso, country_name,
    country_region_iso, event_short_name, status
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);

                """
                
                values = (
                    session['id'],
                    session['date'],
                    session['number'],
                    session['condition']['track'],
                    session['condition']['air'],
                    session['condition']['humidity'],
                    session['condition']['ground'],
                    session['condition']['weather'],
                    session['circuit'],
                    session['session_files'].get('classification', ''),
                    session['session_files'].get('analysis', ''),
                    session['session_files'].get('average_speed', ''),
                    session['session_files'].get('fast_lap_sequence', ''),
                    session['session_files'].get('lap_chart', ''),
                    session['session_files'].get('analysis_by_lap', ''),
                    session['session_files'].get('fast_lap_rider', ''),
                    session['session_files'].get('grid', ''),
                    session['session_files'].get('session', ''),
                    session['session_files'].get('world_standing', ''),
                    session['session_files'].get('best_partial_time', ''),
                    session['session_files'].get('maximum_speed', ''),
                    session['session_files'].get('combined_practice', ''),
                    session['session_files'].get('combined_classification', ''),
                    session['type'],
                    session['category']['id'],
                    session['category']['legacy_id'],
                    session['category']['name'],
                    session['event']['id'],
                    session['event']['name'],
                    session['event']['sponsored_name'],
                    session['event']['season'],
                    session['event']['circuit']['id'],
                    session['event']['circuit']['legacy_id'],
                    session['event']['circuit']['place'],
                    session['event']['circuit']['nation'],
                    session['event']['country']['iso'],
                    session['event']['country']['name'],
                    session['event']['country']['region_iso'],
                    session['event']['short_name'],
                    session['status']
                )
                
                #if insert_query.count('%s') != len(values):
                #    print("Mismatch detected!")
                #    print(f"Placeholders: {insert_query.count('%s')}, Values: {len(values)}")
                #    continue  # Skip this iteration to avoid the error

                try:
                    cursor.execute(insert_query, values)
                    cnx.commit()
                except Exception as e:
                    print("Error:", e)
                    continue
            #    #print("Query:", insert_query)
            #    #print("Values:", values)
            #    #sys.exit(0)
        else:
            print(f"Failed to fetch data from {url}")

        print("***********************************")

    # Don't forget to commit and close your connection after all operations.
#cnx.commit()
cursor.close()
cnx.close()

