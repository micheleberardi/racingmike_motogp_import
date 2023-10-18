import pymysql
import hashlib
import requests
import sys
cnx = pymysql.connect(host='45.56.107.211', user='michelone', passwd='Buc43sede##LLMAUmichelone311walnut',
                      db='racingmike_motogp', cursorclass=pymysql.cursors.DictCursor, use_unicode=True, charset="utf8")

cursor = cnx.cursor()

querySelect = "SELECT * FROM racingmike_motogp.categories_by_season"
cursor.execute(querySelect)
result = cursor.fetchall()
print(result)
for row in result:
    category_id = row['id']
    name = row['name']
    year = row['year']
    print("RUNNING YEAR "+str(year))
    #url = "https://api.motogp.com/riders-api/season/" + str(year) + "/teams?category=" + str(category_id)
    url = "https://api.motogp.pulselive.com/motogp/v1/teams?categoryUuid="+ str(category_id)+"&seasonYear="+str(year)
    print(url)
    response = requests.get(url)
    data = response.json()
    for team in data:
        for rider in team['riders']:
            team_id = None
            if rider.get('current_career_step') and rider['current_career_step'].get('team'):
                team_id = rider['current_career_step']['team'].get('id')
            values = (
                team['id'],
                team['name'],
                team['legacy_id'],
                team['color'],
                team['text_color'],
                team['picture'],
                team['constructor']['id'],
                team['constructor']['name'],
                team['constructor']['legacy_id'],
                rider['id'],
                rider['name'],
                rider['surname'],
                rider['nickname'],
                rider['current_career_step']['season'],
                rider['current_career_step']['number'],
                rider['current_career_step']['sponsored_team'],
                #rider['current_career_step']['team']['id'],
                team_id,
                rider['current_career_step']['category']['id'],
                rider['current_career_step']['category']['name'],
                rider['current_career_step']['category']['legacy_id'],
                rider['current_career_step']['in_grid'],
                rider['current_career_step']['short_nickname'],
                rider['current_career_step']['current'],
                rider['current_career_step']['pictures']['profile']['main'],
                rider['current_career_step']['pictures']['bike']['main'],
                rider['current_career_step']['pictures']['helmet']['main'],
                rider['current_career_step']['pictures']['number'],
                rider['current_career_step']['pictures']['portrait'],
                rider['current_career_step']['type'],
                rider['country']['iso'],
                rider['country']['name'],
                rider['country']['flag'],
                rider['birth_city'],
                rider['birth_date'],
                rider['years_old'],
                rider['published'],
                rider['legacy_id'],
                year
            )

            # Create a string representation of the entry
            entry_str = ''.join(map(str, values))

            # Generate the MD5 hash
            md5_hash = hashlib.md5(entry_str.encode()).hexdigest()

            # Extend your insertion values and query to include the MD5 hash
            values += (md5_hash,)
            try:
                insert_query = "INSERT INTO TeamRiders VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(insert_query, values)
            except Exception as e:
                print(e)
                print(values)
                print(insert_query)
                continue
        cnx.commit()

cursor.close()
cnx.close()

