import pymysql
import hashlib
import requests
import sys

cnx = pymysql.connect(host='45.56.107.211', user='michelone', passwd='Buc43sede##LLMAUmichelone311walnut',
                      db='racingmike_motogp', cursorclass=pymysql.cursors.DictCursor, use_unicode=True, charset="utf8")

cursor = cnx.cursor()

querySelect = "SELECT * FROM racingmike_motogp.seasons where year = 2023"
cursor.execute(querySelect)
result = cursor.fetchall()
for row in result:
    id = row['id']
    year = row['year']
    url = "https://api.motogp.pulselive.com/motogp/v1/results/categories?seasonUuid="+str(id)
    print(url)
    #sys.exit(0)

    response = requests.get(url)
    data = response.json()
    print(data)
    for d in data:
        id = d['id']
        legacy_id = d['legacy_id']
        name = d['name']

        md5= str(id)+str(legacy_id)+str(name)+str(year)
        md5 = hashlib.md5(md5.encode('utf-8')).hexdigest()

        dict_mysql = {"id": id, "legacy_id": legacy_id, "name": name, "year": year, "md5": md5}
        # build column names and placeholders
        columns = []
        placeholders = []
        for key in dict_mysql.keys():
            columns.append(key)
            placeholders.append("%s")

        # convert integer to string
        columns = [str(col) for col in columns]

        # join column names and placeholders into a string
        column_names = ", ".join(columns)
        value_placeholders = ", ".join(placeholders)

        # build insert statement

        try:
            query = f"INSERT INTO categories_general ({column_names}) VALUES ({value_placeholders})"

            # execute insert statement with values
            values = tuple(dict_mysql.values())
            # print(dict_mysql)
            print(query)
            print(values)
            cursor.execute(query, values)
            cnx.commit()
            print("***********************************")
        except Exception as e:
            print(e)


