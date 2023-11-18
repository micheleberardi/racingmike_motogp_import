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


def safe_get(d, *keys):
    """Retrieve nested dictionary values safely."""
    for key in keys:
        if d is None:
            return ""
        d = d.get(key)
    return d


cursor = cnx.cursor()
#querySelect = "SELECT * FROM sessions WHERE event_season BETWEEN 2022 AND 2022 ORDER BY event_season ASC" #where event_season = '2023' and event_id = 'bfd8a08c-cbb4-413a-a210-6d34774ea4c5';
querySelect = "SELECT * FROM sessions where event_season = '2023' and event_id = 'f8346513-918f-4df1-a943-e2492d7835a1'"

cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    session_id = row['id']
    event_id = row['event_id']
    event_year = row['event_season']
    category_id = row['category_id']  # << NEW
    track_condition = row['track_condition']# << NEW
    air_condition = row['air_condition'] # << NEW
    humidity_condition = row['humidity_condition'] # << NEW
    ground_condition = row['ground_condition'] # << NEW
    weather_condition = row['weather_condition'] # << NEW
    session_number = row['number']
    circuit_name = row['circuit_name'] # << NEW
    session_type = row['type'] # << NEW
    category_name = row['category_name'] # << NEW
    event_name = row['event_name']  # << NEW
    event_sponsored_name    = row['event_sponsored_name'] # << NEW
    event_season = row['event_season'] # << NEW
    circuit_id = row['circuit_id'] # << NEW
    circuit_legacy_id = row['circuit_legacy_id'] # << NEW
    circuit_place = row['circuit_place'] # << NEW
    circuit_nation = row['circuit_nation'] # << NEW
    circuit_country_iso = row['country_iso'] # << NEW
    circuit_country_name = row['country_name'] # << NEW
    circuit_country_region_iso = row['country_region_iso'] # << NEW
    event_short_name = row['event_short_name'] # << NEW



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
            rider_number = rider.get("number", None)
            riders_api_uuid = rider.get("riders_api_uuid", None)

            rider_country = rider.get("country", {})
            rider_country_iso = rider_country.get("iso", None)
            rider_country_name = rider_country.get("name", None)
            rider_region_iso = rider_country.get("region_iso", None)

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
            gap_prev = gap.get("prev", None)    #<< NEW

            top_speed = item.get("top_speed",None)  # << NEW
            best_lap = item.get("best_lap", None)
            best_lap_number = best_lap.get("number", None) if best_lap else None # << NEW
            best_lap_time = best_lap.get("time", None) if best_lap else None # << NEW


            total_laps = item.get("total_laps", None)
            time = item.get("time", None)
            points = item.get("points", None)
            status = item.get("status", None)
            file,  # Assuming this is directly given from somewhere else in your code
            files,  # Assuming this is directly given from somewhere else in your code
            session_id,  # Assuming this is directly given from somewhere else in your code
            event_id,  # Assuming this is directly given from somewhere else in your code
            event_year,  # Assuming this is directly given from somewhere else in your code
            category_id = row['category_id']  # << NEW
            track_condition = row['track_condition']  # << NEW
            air_condition = row['air_condition']  # << NEW
            humidity_condition = row['humidity_condition']  # << NEW
            ground_condition = row['ground_condition']  # << NEW
            weather_condition = row['weather_condition']  # << NEW
            print("***********************************")
            print(category_id)
            print("TRACK CONDITION: ", track_condition)
            print("AIR CONDITION: ", air_condition)
            print("HUMIDITY CONDITION: ", humidity_condition)
            print("GROUND CONDITION: ", ground_condition)
            print("WEATHER CONDITION: ", weather_condition)
            print("***********************************")



            # The remaining items seem to be directly available in your code context:
            # file, files, session_id, event_id, event_year, md5

            md5 = rider_id + str(session_id) + str(event_id) + str(event_year)+str(category_id)
            md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
            try:
                insert_query = """
                INSERT INTO results (
                    result_id, position, rider_id, rider_full_name, rider_country_iso, 
                    rider_country_name, rider_region_iso, rider_legacy_id, rider_number,
                    riders_api_uuid, team_id, team_name, team_legacy_id, team_season_id,
                    team_season_year, team_season_current, constructor_id, constructor_name,
                    constructor_legacy_id, average_speed, gap_first, gap_lap, total_laps, 
                    time, points, status, file, files,session_id, event_id,event_year,md5,gap_prev,top_speed,best_lap_number,best_lap_time,track_condition,air_condition,humidity_condition,ground_condition,weather_condition,category_id,session_number,circuit_name,session_type,category_name,event_name,event_sponsored_name,event_season,circuit_id,circuit_legacy_id,circuit_place,circuit_nation,circuit_country_iso,circuit_country_name,circuit_country_region_iso,event_short_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    position = VALUES(position),
                    rider_id = VALUES(rider_id),
                    rider_full_name = VALUES(rider_full_name),
                    rider_country_iso = VALUES(rider_country_iso),
                    rider_country_name = VALUES(rider_country_name),
                    rider_region_iso = VALUES(rider_region_iso),
                    rider_legacy_id = VALUES(rider_legacy_id),
                    rider_number = VALUES(rider_number),
                    riders_api_uuid = VALUES(riders_api_uuid),
                    team_id = VALUES(team_id),
                    team_name = VALUES(team_name),
                    team_legacy_id = VALUES(team_legacy_id),
                    team_season_id = VALUES(team_season_id),
                    team_season_year = VALUES(team_season_year),
                    team_season_current = VALUES(team_season_current),
                    constructor_id = VALUES(constructor_id),
                    constructor_name = VALUES(constructor_name),
                    constructor_legacy_id = VALUES(constructor_legacy_id),
                    average_speed = VALUES(average_speed),
                    gap_first = VALUES(gap_first),
                    gap_lap = VALUES(gap_lap),
                    total_laps = VALUES(total_laps),
                    time = VALUES(time),
                    points = VALUES(points),
                    status = VALUES(status),
                    file = VALUES(file),
                    files = VALUES(files),
                    session_id = VALUES(session_id),
                    event_id = VALUES(event_id),
                    event_year = VALUES(event_year),
                    md5 = VALUES(md5),
                    gap_prev = VALUES(gap_prev),
                    top_speed = VALUES(top_speed),
                    best_lap_number = VALUES(best_lap_number),
                    best_lap_time = VALUES(best_lap_time),
                    track_condition = VALUES(track_condition),
                    air_condition = VALUES(air_condition),
                    humidity_condition = VALUES(humidity_condition),
                    ground_condition = VALUES(ground_condition),
                    weather_condition = VALUES(weather_condition),
                    category_id = VALUES(category_id),
                    session_number = VALUES(session_number),
                    circuit_name = VALUES(circuit_name),
                    session_type = VALUES(session_type),
                    category_name = VALUES(category_name),
                    event_name = VALUES(event_name),
                    event_sponsored_name = VALUES(event_sponsored_name),
                    event_season = VALUES(event_season),
                    circuit_id = VALUES(circuit_id),
                    circuit_legacy_id = VALUES(circuit_legacy_id),
                    circuit_place = VALUES(circuit_place),
                    circuit_nation = VALUES(circuit_nation),
                    circuit_country_iso = VALUES(circuit_country_iso),
                    circuit_country_name = VALUES(circuit_country_name),
                    circuit_country_region_iso = VALUES(circuit_country_region_iso),
                    event_short_name = VALUES(event_short_name)


                """
                cursor.execute(insert_query, (
                    id,
                    position,
                    rider_id,
                    full_name,
                    rider_country_iso,
                    rider_country_name,
                    rider_region_iso,
                    legacy_id,
                    rider_number,
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
                    md5,
                    gap_prev,
                    top_speed,
                    best_lap_number,
                    best_lap_time,
                    track_condition,
                    air_condition,
                    humidity_condition,
                    ground_condition,
                    weather_condition,
                    category_id,
                    session_number,
                    circuit_name,
                    session_type,
                    category_name,
                    event_name,
                    event_sponsored_name,
                    event_season,
                    circuit_id,
                    circuit_legacy_id,
                    circuit_place,
                    circuit_nation,
                    circuit_country_iso,  # Use the correct variable name here
                    circuit_country_name,
                    circuit_country_region_iso,
                    event_short_name
                ))

                cnx.commit()
            except TypeError as e:
                if str(e) == "'NoneType' object is not subscriptable":
                    print(item)
                    sys.exit(0)
                print("ca" +str(e))
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


            md5 = rider_id + str(session_id) + str(event_id) + str(event_year)+str(category_id)
            md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
            print("CATEGORY ID: ", category_id)

            try:
                insert_record_query = """
                    INSERT INTO records (
                        record_type, rider_id, rider_full_name, rider_country_iso, 
                        rider_country_name, rider_region_iso, rider_legacy_id, bestLap_number, 
                        bestLap_time, speed, record_year, isNewRecord, event_id, md5, category_id, category_name, event_name, event_sponsored_name, event_season, circuit_id, circuit_legacy_id, circuit_place, circuit_nation, circuit_country_iso, circuit_country_name, circuit_country_region_iso, event_short_name,session_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    record_type = VALUES(record_type),
                    rider_id = VALUES(rider_id),
                    rider_full_name = VALUES(rider_full_name),
                    rider_country_iso = VALUES(rider_country_iso),
                    rider_country_name = VALUES(rider_country_name),
                    rider_region_iso = VALUES(rider_region_iso),
                    rider_legacy_id = VALUES(rider_legacy_id),
                    bestLap_number = VALUES(bestLap_number),
                    bestLap_time = VALUES(bestLap_time),
                    speed = VALUES(speed),
                    record_year = VALUES(record_year),
                    isNewRecord = VALUES(isNewRecord),
                    event_id = VALUES(event_id),
                    md5 = VALUES(md5),
                    category_id = VALUES(category_id),
                    category_name = VALUES(category_name),
                    event_name = VALUES(event_name),
                    event_sponsored_name = VALUES(event_sponsored_name),
                    event_season = VALUES(event_season),
                    circuit_id = VALUES(circuit_id),
                    circuit_legacy_id = VALUES(circuit_legacy_id),
                    circuit_place = VALUES(circuit_place),
                    circuit_nation = VALUES(circuit_nation),
                    circuit_country_iso = VALUES(circuit_country_iso),
                    circuit_country_name = VALUES(circuit_country_name),
                    circuit_country_region_iso = VALUES(circuit_country_region_iso),
                    event_short_name = VALUES(event_short_name),
                    session_id = VALUES(session_id)
                   
                     
                """
                cursor.execute(insert_record_query, (
                    record_type, rider_id, rider_full_name, rider_country_iso,
                    rider_country_name, rider_region_iso, rider_legacy_id, best_lap_number,
                    best_lap_time, speed, year, is_new_record, event_id, md5, category_id, category_name, event_name, event_sponsored_name, event_season, circuit_id, circuit_legacy_id, circuit_place, circuit_nation, circuit_country_iso, circuit_country_name, circuit_country_region_iso, event_short_name,session_id
                ))
                cnx.commit()
            except TypeError as e:
                cnx.rollback()
                print("CaazziA TypeError occurred:", e)
                sys.exit(0)
                # Log the error, and/or print more diagnostic information

    print("TIME TO SLEEP")
    time_module.sleep(2)

#print("TIME TO SLEEP")
#time.sleep(20)
# Always close the cursor and connection
cursor.close()
cnx.close()
print("DONE")
#time.sleep(20)
