import pymysql
import hashlib
import requests
import json
import sys
import os
from dotenv import load_dotenv
import logging

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

try:
    #querySelect = "SELECT * FROM events"
    querySelect = "SELECT * FROM racingmike_motogp.events WHERE year = '2025' AND test != 1 AND date_start BETWEEN CURDATE() - INTERVAL 10 DAY AND CURDATE() ORDER BY date_start DESC;"
    cursor.execute(querySelect)
    result = cursor.fetchall()
    logging.info(f"Number of events found: {len(result)}")
    for row in result:
        event_id = row['id']
        year = row['year']
        logging.info("RUNNING YEAR "+str(year))
        querySelect3 = "SELECT * FROM racingmike_motogp.categories_general WHERE year = "+str(year)
        #querySelect3 = "SELECT * FROM sessions where year = 2025 AND date BETWEEN CURDATE() - INTERVAL 40 DAY AND CURDATE() ORDER BY date DESC"
        logging.info(querySelect3)
        cursor.execute(querySelect3)
        categories = cursor.fetchall()
        logging.info(f"Number of categories processed: {len(categories)}")
        for row in categories:
            category_id = row['id']
            name = row['name']
            year = row['year']
            url = "https://api.motogp.pulselive.com/motogp/v1/results/sessions?eventUuid="+str(event_id)+"&categoryUuid="+str(category_id)
            logging.info(url)
            logging.info("***********************************")
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                logging.info(f"API returned {len(data)} sessions")
                # Assuming the JSON response is a list
                sessions_inserted = 0
                for session in data:
                    # Constructing the INSERT query using the fetched data
                    insert_query = """
                    INSERT INTO sessions (
                        id, date, number, track_condition, air_condition, humidity_condition, ground_condition,
                        weather_condition, circuit_name, classification_url, classification_menu_position,
                        analysis_url, analysis_menu_position, average_speed_url, average_speed_menu_position,
                        fast_lap_sequence_url, fast_lap_sequence_menu_position, lap_chart_url, lap_chart_menu_position,
                        analysis_by_lap_url, analysis_by_lap_menu_position, fast_lap_rider_url, fast_lap_rider_menu_position,
                        grid_url, grid_menu_position, session_url, session_menu_position, world_standing_url,
                        world_standing_menu_position, best_partial_time_url, best_partial_time_menu_position,
                        maximum_speed_url, maximum_speed_menu_position, combined_practice_url, combined_practice_menu_position,
                        combined_classification_url, combined_classification_menu_position, type, category_id,
                        category_legacy_id, category_name, event_id, event_name, event_sponsored_name, year,
                        circuit_id, circuit_legacy_id, circuit_place, circuit_nation, country_iso, country_name,
                        country_region_iso, event_short_name, status
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE status=VALUES(status), date=VALUES(date);
                    """

                    session_files = session.get('session_files', {})
                    condition = session.get('condition', {})
                    category = session.get('category', {})
                    event = session.get('event', {})
                    event_circuit = event.get('circuit', {})
                    event_country = event.get('country', {})

                    values = (
                        session.get('id'),
                        session.get('date'),
                        session.get('number'),
                        condition.get('track'),
                        condition.get('air'),
                        condition.get('humidity'),
                        condition.get('ground'),
                        condition.get('weather'),
                        session.get('circuit'),
                        session_files.get('classification', {}).get('url'),
                        session_files.get('classification', {}).get('menu_position'),
                        session_files.get('analysis', {}).get('url'),
                        session_files.get('analysis', {}).get('menu_position'),
                        session_files.get('average_speed', {}).get('url'),
                        session_files.get('average_speed', {}).get('menu_position'),
                        session_files.get('fast_lap_sequence', {}).get('url'),
                        session_files.get('fast_lap_sequence', {}).get('menu_position'),
                        session_files.get('lap_chart', {}).get('url'),
                        session_files.get('lap_chart', {}).get('menu_position'),
                        session_files.get('analysis_by_lap', {}).get('url'),
                        session_files.get('analysis_by_lap', {}).get('menu_position'),
                        session_files.get('fast_lap_rider', {}).get('url'),
                        session_files.get('fast_lap_rider', {}).get('menu_position'),
                        session_files.get('grid', {}).get('url'),
                        session_files.get('grid', {}).get('menu_position'),
                        session_files.get('session', {}).get('url'),
                        session_files.get('session', {}).get('menu_position'),
                        session_files.get('world_standing', {}).get('url'),
                        session_files.get('world_standing', {}).get('menu_position'),
                        session_files.get('best_partial_time', {}).get('url'),
                        session_files.get('best_partial_time', {}).get('menu_position'),
                        session_files.get('maximum_speed', {}).get('url'),
                        session_files.get('maximum_speed', {}).get('menu_position'),
                        session_files.get('combined_practice', {}).get('url'),
                        session_files.get('combined_practice', {}).get('menu_position'),
                        session_files.get('combined_classification', {}).get('url'),
                        session_files.get('combined_classification', {}).get('menu_position'),
                        session.get('type'),
                        category.get('id'),
                        category.get('legacy_id'),
                        category.get('name'),
                        event.get('id'),
                        event.get('name'),
                        event.get('sponsored_name'),
                        event.get('season'),
                        event_circuit.get('id'),
                        event_circuit.get('legacy_id'),
                        event_circuit.get('place'),
                        event_circuit.get('nation'),
                        event_country.get('iso'),
                        event_country.get('name'),
                        event_country.get('region_iso'),
                        event.get('short_name'),
                        session.get('status')
                    )

                    try:
                        cursor.execute(insert_query, values)
                        cnx.commit()
                        sessions_inserted += 1
                    except Exception as e:
                        logging.error(f"Error inserting session {session.get('id')}: {e}")
                        continue
                logging.info(f"Sessions inserted/updated: {sessions_inserted}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch data from {url}: {e}")

            logging.info("***********************************")

finally:
    cursor.close()
    cnx.close()
