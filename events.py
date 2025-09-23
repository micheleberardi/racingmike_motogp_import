import logging
import pymysql
import requests
import sys
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logging.info(f"DB_HOST: {os.getenv('DB_HOST')}")



cnx = None
cursor = None
try:
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

    querySelect = "SELECT * FROM seasons where year =2025"
    cursor.execute(querySelect)
    result = cursor.fetchall()

    for row in result:
        season_id = row['id']
        year = row['year']
        url = f"https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid={season_id}&isFinished=true"
        logging.info(f"Requesting events for season {year} from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error fetching events for season {year}: {e}")
            continue
        events = response.json()

        for event in events:
            try:
                insert_event_query = """
                    INSERT INTO events (id, country_iso, country_name, country_region_iso,
                                    event_circuit_information_url, event_circuit_information_menu_position,
                                    event_podiums_url, event_podiums_menu_position,
                                    event_pole_positions_url, event_pole_positions_menu_position,
                                    event_nations_statistics_url, event_nations_statistics_menu_position,
                                    event_riders_all_time_url, event_riders_all_time_menu_position,
                                    circuit_id, circuit_name, circuit_legacy_id,
                                    circuit_place, circuit_nation, test, sponsored_name,
                                    date_end, toad_api_uuid, date_start, name,
                                    season_id, year, season_current, short_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name=VALUES(name),
                        date_start=VALUES(date_start),
                        date_end=VALUES(date_end)
                """
                event_files = event.get('event_files', {})
                circuit_info = event_files.get('circuit_information', {})
                podiums = event_files.get('podiums', {})
                pole_positions = event_files.get('pole_positions', {})
                nations_statistics = event_files.get('nations_statistics', {})
                riders_all_time = event_files.get('riders_all_time', {})
                circuit = event.get('circuit', {})
                season = event.get('season', {})
                cursor.execute(insert_event_query, (
                    event.get('id'),
                    event.get('country', {}).get('iso'),
                    event.get('country', {}).get('name'),
                    event.get('country', {}).get('region_iso'),
                    circuit_info.get('url'),
                    circuit_info.get('menu_position'),
                    podiums.get('url'),
                    podiums.get('menu_position'),
                    pole_positions.get('url'),
                    pole_positions.get('menu_position'),
                    nations_statistics.get('url'),
                    nations_statistics.get('menu_position'),
                    riders_all_time.get('url'),
                    riders_all_time.get('menu_position'),
                    circuit.get('id'),
                    circuit.get('name'),
                    circuit.get('legacy_id'),
                    circuit.get('place'),
                    circuit.get('nation'),
                    event.get('test'),
                    event.get('sponsored_name'),
                    event.get('date_end'),
                    event.get('toad_api_uuid'),
                    event.get('date_start'),
                    event.get('name'),
                    season.get('id'),
                    season.get('year'),
                    season.get('current'),
                    event.get('short_name')
                ))
                logging.info(f"Inserted/Updated event {event.get('id')} ({event.get('name')})")
                cnx.commit()

                # Inserting legacy IDs
                for legacy in event.get('legacy_id', []):
                    insert_legacy_query = """
                        INSERT INTO event_legacy_ids (event_id, categoryId, eventId)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE eventId=VALUES(eventId)
                    """
                    cursor.execute(insert_legacy_query, (event.get('id'), legacy.get('categoryId'), legacy.get('eventId')))
                    logging.info(f"Inserted/Updated legacy ID for event {event.get('id')}: categoryId {legacy.get('categoryId')}, eventId {legacy.get('eventId')}")
                cnx.commit()
            except Exception as e:
                logging.error(f"Error: {e}")
                continue
finally:
    if cursor:
        cursor.close()
    if cnx:
        cnx.close()
