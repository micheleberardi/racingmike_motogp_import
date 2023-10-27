#1955
import os
import pymysql
import hashlib
from datetime import datetime, timedelta, time
import sys
import requests
import json
import time
import time as time_module

cnx = pymysql.connect(host='45.56.107.211', user='michelone', passwd='Buc43sede##LLMAUmichelone311walnut',
                      db='racingmike_motogp', cursorclass=pymysql.cursors.DictCursor, use_unicode=True, charset="utf8")


def safe_get(d, *keys):
    """Retrieve nested dictionary values safely."""
    for key in keys:
        if d is None:
            return ""
        d = d.get(key)
    return d


cursor = cnx.cursor()
#querySelect = "SELECT * FROM sessions WHERE event_season BETWEEN 1991 AND 2022 ORDER BY event_season ASC" #where event_season = '2023' and event_id = 'bfd8a08c-cbb4-413a-a210-6d34774ea4c5';"
querySelect = "SELECT * FROM sessions where event_season = '2023' and event_id = '874cada0-7330-44c1-aa54-d15b97d3cd62';"

cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    session_id = row['id']
    event_id = row['event_id']
    event_year = row['event_season']

    print("RUNNING SESSION "+str(event_year)+" "+str(session_id))
    print("***********************************")
    url = "https://api.motogp.pulselive.com/motogp/v1/results/session/" + str(session_id) + "/classification?test=false"
    print(url)
    #sys.exit(0)

    # Removed the sys.exit(0) as it stops the script execution
    response = requests.get(url)
    data = response.json()

    if 'classification' in data:
        classifications = data['classification']
        file = data.get('file', '')
        files = data.get('files', '')
        records = data.get('records', '')

        for item in classifications:
            id = item.get("id", None)
            position = item.get("position", None)

            rider = item.get("rider", {})
            rider_id = rider.get("id", None)
            full_name = rider.get("full_name", None)
            legacy_id = rider.get("legacy_id", None)
            number = rider.get("number", None)
            riders_api_uuid = rider.get("riders_api_uuid", None)

            country = rider.get("country", {})
            country_iso = country.get("iso", None)
            country_name = country.get("name", None)
            region_iso = country.get("region_iso", None)

            team = item.get("team", {})
            team_id = team.get("id", None) if team else None
            team_name = team.get("name", None) if team else None
            team_legacy_id = team.get("legacy_id", None) if team else None

            season = team.get("season", {}) if team else {}
            season_id = season.get("id", None) if season else None
            season_year = season.get("year", None) if season else None
            season_current = season.get("current", None) if season else None

            constructor = item.get("constructor", {})
            constructor_id = constructor.get("id", None) if constructor else None
            constructor_name = constructor.get("name", None) if constructor else None
            constructor_legacy_id = constructor.get("legacy_id", None) if constructor else None

            average_speed = item.get("average_speed", None)

            gap = item.get("gap", {})
            gap_first = gap.get("first", None)
            gap_lap = gap.get("lap", None)

            total_laps = item.get("total_laps", None)
            time = item.get("time", None)
            points = item.get("points", None)
            status = item.get("status", None)
            file,  # Assuming this is directly given from somewhere else in your code
            files,  # Assuming this is directly given from somewhere else in your code
            session_id,  # Assuming this is directly given from somewhere else in your code
            event_id,  # Assuming this is directly given from somewhere else in your code
            event_year,  # Assuming this is directly given from somewhere else in your code

            # The remaining items seem to be directly available in your code context:
            # file, files, session_id, event_id, event_year, md5

            md5 = (
                    str(safe_get(item, 'id')) +
                    str(safe_get(item, 'rider', 'id')) +
                    str(safe_get(item, 'team', 'id')) +
                    str(safe_get(item, 'constructor', 'id')) +
                    str(safe_get(item, 'team', 'season', 'year')) +
                    str(session_id)
            )
            md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
            try:
                insert_query = """
                INSERT INTO results (
                    result_id, position, rider_id, rider_full_name, rider_country_iso, 
                    rider_country_name, rider_region_iso, rider_legacy_id, rider_number,
                    riders_api_uuid, team_id, team_name, team_legacy_id, team_season_id,
                    team_season_year, team_season_current, constructor_id, constructor_name,
                    constructor_legacy_id, average_speed, gap_first, gap_lap, total_laps, 
                    time, points, status, file, files,session_id, event_id,event_year,md5
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE md5 = VALUES(md5);
                """
                cursor.execute(insert_query, (
                    id,
                    position,
                    rider_id,
                    full_name,
                    country_iso,
                    country_name,
                    region_iso,
                    legacy_id,
                    number,
                    riders_api_uuid,
                    team_id,
                    team_name,
                    team_legacy_id,
                    season_id,
                    season_year,
                    season_current,
                    constructor_id,
                    constructor_name,
                    constructor_legacy_id,
                    average_speed,
                    gap_first,
                    gap_lap,
                    total_laps,
                    time,
                    points,
                    status,
                    file,
                    files,
                    session_id,
                    event_id,
                    event_year,
                    md5
                ))

                cnx.commit()
            except TypeError as e:
                if str(e) == "'NoneType' object is not subscriptable":
                    print(item)
                sys.exit(0)
                #continue

        # Inserting the records
        #md5 = records[0]['rider']['id']+str(records[0]['bestLap']['number'])+str(records[0]['bestLap']['time'])+str(records[0]['speed'])+str(records[0]['year'])+str(session_id)
        #md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
        #records = [{"type": "poleLap", "rider": {"id": "3c7598e5-12ec-4e19-9311-1c0e9a017cbe", "full_name": "Diogo Moreira", "country": {"iso": "BR", "name": "Brazil", "region_iso": ""}, "legacy_id": 10257 }, "bestLap": {"number": null, "time": "01:39.0850"}, "speed": "156.2", "year": null, "isNewRecord": false } ]
        for record in records:
            record_type = record['type']
            rider_id = record['rider']['id']
            rider_full_name = record['rider']['full_name']
            rider_country_iso = record['rider']['country']['iso']
            rider_country_name = record['rider']['country']['name']
            rider_region_iso = record['rider']['country']['region_iso']
            rider_legacy_id = record['rider']['legacy_id']
            best_lap_number = record['bestLap']['number']
            best_lap_time = record['bestLap']['time']
            speed = record['speed']
            year = record['year']
            is_new_record = record['isNewRecord']
            md5 = records[0]['rider']['id'] + str(records[0]['year']) + str(session_id)+str(event_id)+str(record_type)
            md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()

            try:
                insert_record_query = """
                    INSERT INTO records (
                        session_id, type, rider_id, rider_full_name, rider_country_iso, 
                        rider_country_name, rider_region_iso, rider_legacy_id, bestLap_number, 
                        bestLap_time, speed, record_year, isNewRecord, event_id,md5
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE md5 = VALUES(md5);
                """
                cursor.execute(insert_record_query, (
                    session_id, record_type, rider_id, rider_full_name, rider_country_iso,
                    rider_country_name, rider_region_iso, rider_legacy_id, best_lap_number,
                    best_lap_time, speed, year, is_new_record, event_id,md5
                ))
                cnx.commit()
            except TypeError as e:
                if str(e) == "'NoneType' object is not subscriptable":
                    print(item)
                sys.exit(0)
                # continue
    print("TIME TO SLEEP")
    time_module.sleep(10)

#print("TIME TO SLEEP")
#time.sleep(20)
# Always close the cursor and connection
cursor.close()
cnx.close()
print("DONE")
#time.sleep(20)
