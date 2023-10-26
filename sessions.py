import pymysql
import hashlib
import requests
import json
import sys
cnx = pymysql.connect(host='45.56.107.211', user='michelone', passwd='Buc43sede##LLMAUmichelone311walnut',
                      db='racingmike_motogp', cursorclass=pymysql.cursors.DictCursor, use_unicode=True, charset="utf8")

cursor = cnx.cursor()

#querySelect = "SELECT * FROM events"
querySelect = "SELECT * FROM racingmike_motogp.events where season_year = 2023 and id = '0bb1a25b-ed29-4e93-8460-214bed97e632';"
cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    event_id = row['id']
    year = row['season_year']
    print("RUNNING YEAR "+str(year))
    #querySelect3 = "SELECT * FROM racingmike_motogp.categories_general WHERE year = "+str(year)
    querySelect3 = "SELECT * FROM racingmike_motogp.categories_general"
    #print(querySelect3)
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
            # Assuming the JSON response is a list
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
                    category_legacy_id, category_name, event_id, event_name, event_sponsored_name, event_season,
                    circuit_id, circuit_legacy_id, circuit_place, circuit_nation, country_iso, country_name, 
                    country_region_iso, event_short_name, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
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
                    session['session_files']['classification']['url'],
                    session['session_files']['classification']['menu_position'],
                    session['session_files']['analysis']['url'],
                    session['session_files']['analysis']['menu_position'],
                    session['session_files']['average_speed']['url'],
                    session['session_files']['average_speed']['menu_position'],
                    session['session_files']['fast_lap_sequence']['url'],
                    session['session_files']['fast_lap_sequence']['menu_position'],
                    session['session_files']['lap_chart']['url'],
                    session['session_files']['lap_chart']['menu_position'],
                    session['session_files']['analysis_by_lap']['url'],
                    session['session_files']['analysis_by_lap']['menu_position'],
                    session['session_files']['fast_lap_rider']['url'],
                    session['session_files']['fast_lap_rider']['menu_position'],
                    session['session_files']['grid']['url'],
                    session['session_files']['grid']['menu_position'],
                    session['session_files']['session']['url'],
                    session['session_files']['session']['menu_position'],
                    session['session_files']['world_standing']['url'],
                    session['session_files']['world_standing']['menu_position'],
                    session['session_files']['best_partial_time']['url'],
                    session['session_files']['best_partial_time']['menu_position'],
                    session['session_files']['maximum_speed']['url'],
                    session['session_files']['maximum_speed']['menu_position'],
                    session['session_files']['combined_practice']['url'],
                    session['session_files']['combined_practice']['menu_position'],
                    session['session_files']['combined_classification']['url'],
                    session['session_files']['combined_classification']['menu_position'],
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

