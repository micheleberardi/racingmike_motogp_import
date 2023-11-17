import pymysql
import hashlib
import requests
import json
import sys
import hashlib
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

querySelect = "SELECT * FROM racingmike_motogp.seasons order by year desc"
querySelect = "SELECT * FROM racingmike_motogp.seasons where year = 2023"
cursor.execute(querySelect)
result = cursor.fetchall()

for row in result:
    session_id = row['id']
    year = row['year']
    print("*****"+str(year)+"******")
    querySelect3 = "SELECT * FROM categories_general where year ="+str(year)
    cursor.execute(querySelect3)
    categories = cursor.fetchall()
    for r in categories:
        category_id = r['id']
        name = r['name']
        year = r['year']
        url = "https://api.motogp.pulselive.com/motogp/v1/results/standings?seasonUuid=" + str(session_id) + "&categoryUuid=" + str(category_id)
        print(url)
        response = requests.get(url)
        dataResponse = response.status_code

        if dataResponse == 200:
            data = response.json()
            for item in data['classification']:
                rider = item['rider']
                constructor = item['constructor']

                # Check if team and season exist
                team_id, team_name, team_legacy_id, season_id, season_year, season_current = (
                None, None, None, None, None, None)
                if 'team' in item and item['team']:
                    team = item['team']
                    team_id = team['id']
                    team_name = team['name']
                    team_legacy_id = team['legacy_id']

                    if 'season' in team:
                        season = team['season']
                        season_id = season['id']
                        season_year = season['year']
                        season_current = season['current']

                md5 = rider['id']+str(team_id)+str(constructor['id'])+str(year)+str(session_id)+str(category_id)
                md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()
                #print(md5)
                try:
                    cursor.execute(
                        """
                        INSERT INTO standing_riders 
                        (classification_id, position, rider_id, rider_full_name, rider_country_iso, rider_country_name, rider_region_iso, rider_legacy_id, rider_number, riders_api_uuid, team_id, team_name, team_legacy_id, season_id, season_year, season_current, constructor_id, constructor_name, constructor_legacy_id, session, points, xmlFile,year,md5,session_id,category_id)
                        VALUES 
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                        position=VALUES(position), rider_full_name=VALUES(rider_full_name), rider_country_iso=VALUES(rider_country_iso), rider_country_name=VALUES(rider_country_name), rider_region_iso=VALUES(rider_region_iso), rider_legacy_id=VALUES(rider_legacy_id), rider_number=VALUES(rider_number), riders_api_uuid=VALUES(riders_api_uuid), team_id=VALUES(team_id), team_name=VALUES(team_name), team_legacy_id=VALUES(team_legacy_id), season_id=VALUES(season_id), season_year=VALUES(season_year), season_current=VALUES(season_current), constructor_id=VALUES(constructor_id), constructor_name=VALUES(constructor_name), constructor_legacy_id=VALUES(constructor_legacy_id), session=VALUES(session), points=VALUES(points), xmlFile=VALUES(xmlFile), year=VALUES(year), md5=VALUES(md5), session_id=VALUES(session_id), category_id=VALUES(category_id)
                        """,
                        (item['id'], item['position'], rider['id'], rider['full_name'], rider['country']['iso'],
                        rider['country']['name'],
                        rider['country']['region_iso'], rider['legacy_id'], rider.get('number', None),
                        rider['riders_api_uuid'],
                        team_id, team_name, team_legacy_id, season_id, season_year, season_current, constructor['id'],
                        constructor['name'], constructor['legacy_id'],
                        item['session'], item['points'], data['xmlFile'], year, md5, session_id, category_id)
                    )

                    cnx.commit()
                except Exception as e:
                    print(e)
                    sys.exit(0)
                    #cnx.rollback()