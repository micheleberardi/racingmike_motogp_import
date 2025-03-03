import pymysql
import requests
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# print load_dotenv()
print (os.getenv('DB_HOST'))


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
    print(url)
    response = requests.get(url)
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
            """
            cursor.execute(insert_event_query, (
                event['id'], event['country']['iso'], event['country']['name'], event['country']['region_iso'],
                event['event_files']['circuit_information']['url'],
                event['event_files']['circuit_information']['menu_position'],
                event['event_files']['podiums']['url'], event['event_files']['podiums']['menu_position'],
                event['event_files']['pole_positions']['url'], event['event_files']['pole_positions']['menu_position'],
                event['event_files']['nations_statistics']['url'],
                event['event_files']['nations_statistics']['menu_position'],
                event['event_files']['riders_all_time']['url'], event['event_files']['riders_all_time']['menu_position'],
                event['circuit']['id'], event['circuit']['name'], event['circuit']['legacy_id'],
                event['circuit']['place'], event['circuit']['nation'], event['test'], event['sponsored_name'],
                event['date_end'], event['toad_api_uuid'], event['date_start'], event['name'],
                event['season']['id'], event['season']['year'], event['season']['current'], event['short_name']
            ))

            # Inserting legacy IDs
            for legacy in event['legacy_id']:
                insert_legacy_query = """
                    INSERT INTO event_legacy_ids (event_id, categoryId, eventId) 
                    VALUES (%s, %s, %s)
                """
                cursor.execute(insert_legacy_query, (event['id'], legacy['categoryId'], legacy['eventId']))
        except Exception as e:
            print(e)
            continue
    cnx.commit()

cursor.close()
cnx.close()
