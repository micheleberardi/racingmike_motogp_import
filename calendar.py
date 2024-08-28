import json
import pymysql
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
# Connessione al database MySQL
cnx = pymysql.connect(
	host=os.getenv('DB_HOST'),
	user=os.getenv('DB_USER'),
	passwd=os.getenv('DB_PASSWD'),
	db=os.getenv('DB_NAME'),
	cursorclass=pymysql.cursors.DictCursor,
	use_unicode=True,
	charset=os.getenv('DB_CHARSET')
)



# Funzione per inserire i dati nella tabella principale
def insert_main_data(cursor, data):
	insert_query = """
    INSERT INTO MotoGP_Calendar (id, shortname, name, hashtag, circuit, country_code, country,
        start_date, end_date, local_tz_offset, test, has_timing, friendly_name, dates, last_session_end_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
	
	for event in data['calendar']:
		cursor.execute(insert_query, (
			event['id'], event['shortname'], event['name'], event['hashtag'], event['circuit'],
			event['country_code'], event['country'], event['start_date'], event['end_date'],
			event['local_tz_offset'], event['test'], event['has_timing'], event['friendly_name'],
			event['dates'], event['last_session_end_time']
		))


# Funzione per inserire le sessioni chiave nella tabella delle sessioni
def insert_session_data(cursor, event_id, sessions):
	insert_query = """
    INSERT INTO MotoGP_KeySessionTimes (event_id, session_shortname, session_name, start_datetime_utc)
    VALUES (%s, %s, %s, %s)
    """
	
	for session in sessions:
		cursor.execute(insert_query, (
			event_id, session['session_shortname'], session['session_name'], session['start_datetime_utc']
		))


# Leggi il JSON dai dati forniti
json_data = """

{
"season": 2024,
"calendar": [
{
"id": 481,
"shortname": "VC1",
"name": "Valencia MotoGP™ Official Test ",
"hashtag": "#ValenciaTest",
"circuit": "Circuit Ricardo Tormo",
"country_code": "ES",
"country": "Spain",
"start_date": "2023-11-28T00:00:00.000000Z",
"end_date": "2023-11-28T00:00:00.000000Z",
"local_tz_offset": 1,
"test": 1,
"has_timing": 1,
"friendly_name": "Valencia Test",
"dates": "28 Nov",
"key_session_times": [],
"last_session_end_time": "2023-11-28T17:00:00.000000Z"
},
{
"id": 482,
"shortname": "MY1",
"name": "Sepang Shakedown MotoGP™ Official Test",
"hashtag": "#SepangShakedown",
"circuit": "Petronas Sepang International Circuit",
"country_code": "MY",
"country": "Malaysia",
"start_date": "2024-02-01T00:00:00.000000Z",
"end_date": "2024-02-03T00:00:00.000000Z",
"local_tz_offset": 8,
"test": 1,
"has_timing": 0,
"friendly_name": "Sepang Shakedown",
"dates": "01 Feb - 03 Feb",
"key_session_times": [],
"last_session_end_time": "2024-02-03T10:15:00.000000Z"
},
{
"id": 483,
"shortname": "MY2",
"name": "Sepang MotoGP™ Official Test",
"hashtag": "#SepangTest",
"circuit": "Petronas Sepang International Circuit",
"country_code": "MY",
"country": "Malaysia",
"start_date": "2024-02-06T00:00:00.000000Z",
"end_date": "2024-02-08T00:00:00.000000Z",
"local_tz_offset": 8,
"test": 1,
"has_timing": 1,
"friendly_name": "Sepang Test",
"dates": "06 Feb - 08 Feb",
"key_session_times": [],
"last_session_end_time": "2024-02-08T11:00:00.000000Z"
},
{
"id": 484,
"shortname": "QA1",
"name": "Qatar MotoGP™ Official Test",
"hashtag": "#QatarTest",
"circuit": "Lusail International Circuit",
"country_code": "QA",
"country": "Qatar",
"start_date": "2024-02-19T00:00:00.000000Z",
"end_date": "2024-02-20T00:00:00.000000Z",
"local_tz_offset": 3,
"test": 1,
"has_timing": 1,
"friendly_name": "Qatar Test",
"dates": "19 Feb - 20 Feb",
"key_session_times": [],
"last_session_end_time": "2024-02-20T19:00:00.000000Z"
},
{
"id": 485,
"shortname": "PT1",
"name": "Portimao MotoE™ Official Test ",
"hashtag": "#PortimaoTest",
"circuit": "Autódromo Internacional do Algarve",
"country_code": "PT",
"country": "Portugal",
"start_date": "2024-02-21T00:00:00.000000Z",
"end_date": "2024-02-23T00:00:00.000000Z",
"local_tz_offset": 0,
"test": 1,
"has_timing": 0,
"friendly_name": "Portimao Test",
"dates": "21 Feb - 23 Feb",
"key_session_times": [],
"last_session_end_time": "2024-02-23T17:30:00.000000Z"
},
{
"id": 486,
"shortname": "ES2",
"name": "Jerez Moto2™ & Moto3™ Official Test ",
"hashtag": "#JerezTest",
"circuit": "Circuito de Jerez - Ángel Nieto",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-02-28T00:00:00.000000Z",
"end_date": "2024-03-01T00:00:00.000000Z",
"local_tz_offset": 1,
"test": 1,
"has_timing": 1,
"friendly_name": "Jerez Test",
"dates": "28 Feb - 01 Mar",
"key_session_times": [],
"last_session_end_time": "2024-03-01T17:00:00.000000Z"
},
{
"id": 487,
"shortname": "QAT",
"name": "Qatar Airways Grand Prix of Qatar",
"hashtag": "#QatarGP",
"circuit": "Lusail International Circuit",
"country_code": "QA",
"country": "Qatar",
"start_date": "2024-03-08T00:00:00.000000Z",
"end_date": "2024-03-10T00:00:00.000000Z",
"local_tz_offset": 3,
"test": 0,
"has_timing": 1,
"friendly_name": "Qatar GP",
"dates": "08 Mar - 10 Mar",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-03-09T16:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-03-10T17:00:00+00:00"
}
],
"last_session_end_time": "2024-03-10T19:30:00.000000Z"
},
{
"id": 488,
"shortname": "POR",
"name": "Grande Prémio Tissot de Portugal",
"hashtag": "#PortugueseGP",
"circuit": "Autódromo Internacional do Algarve",
"country_code": "PT",
"country": "Portugal",
"start_date": "2024-03-22T00:00:00.000000Z",
"end_date": "2024-03-24T00:00:00.000000Z",
"local_tz_offset": 0,
"test": 0,
"has_timing": 1,
"friendly_name": "Portuguese GP",
"dates": "22 Mar - 24 Mar",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-03-23T15:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-03-24T14:00:00+00:00"
}
],
"last_session_end_time": "2024-03-24T16:30:00.000000Z"
},
{
"id": 489,
"shortname": "AME",
"name": "Red Bull Grand Prix of The Americas",
"hashtag": "#AmericasGP",
"circuit": "Circuit Of The Americas",
"country_code": "US",
"country": "United States",
"start_date": "2024-04-12T00:00:00.000000Z",
"end_date": "2024-04-14T00:00:00.000000Z",
"local_tz_offset": -5,
"test": 0,
"has_timing": 1,
"friendly_name": "Americas GP",
"dates": "12 Apr - 14 Apr",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-04-13T20:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-04-14T19:00:00+00:00"
}
],
"last_session_end_time": "2024-04-14T21:30:00.000000Z"
},
{
"id": 490,
"shortname": "SPA",
"name": "Gran Premio Estrella Galicia 0,0 de España",
"hashtag": "#SpanishGP",
"circuit": "Circuito de Jerez - Ángel Nieto",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-04-26T00:00:00.000000Z",
"end_date": "2024-04-28T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Spanish GP",
"dates": "26 Apr - 28 Apr",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-04-27T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-04-28T12:00:00+00:00"
}
],
"last_session_end_time": "2024-04-28T14:30:00.000000Z"
},
{
"id": 491,
"shortname": "ES1",
"name": "Jerez MotoGP™ Official Test",
"hashtag": "#JerezTest",
"circuit": "Circuito de Jerez - Ángel Nieto",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-04-29T00:00:00.000000Z",
"end_date": "2024-04-29T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 1,
"has_timing": 1,
"friendly_name": "Jerez Test",
"dates": "29 Apr",
"key_session_times": [],
"last_session_end_time": "2024-04-29T16:00:00.000000Z"
},
{
"id": 511,
"shortname": "ES3",
"name": "Jerez Moto2™ & Moto3™ Official Test II",
"hashtag": "#JerezTest",
"circuit": "Circuito de Jerez - Ángel Nieto",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-04-30T00:00:00.000000Z",
"end_date": "2024-04-30T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 1,
"has_timing": 1,
"friendly_name": "Jerez Test",
"dates": "30 Apr",
"key_session_times": [],
"last_session_end_time": "2024-04-30T16:00:00.000000Z"
},
{
"id": 492,
"shortname": "FRA",
"name": "Michelin® Grand Prix de France",
"hashtag": "#FrenchGP",
"circuit": "Le Mans",
"country_code": "FR",
"country": "France",
"start_date": "2024-05-10T00:00:00.000000Z",
"end_date": "2024-05-12T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "French GP",
"dates": "10 May - 12 May",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-05-11T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-05-12T12:00:00+00:00"
}
],
"last_session_end_time": "2024-05-12T14:30:00.000000Z"
},
{
"id": 493,
"shortname": "CAT",
"name": "Gran Premi Monster Energy de Catalunya",
"hashtag": "#CatalanGP",
"circuit": "Circuit de Barcelona-Catalunya",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-05-24T00:00:00.000000Z",
"end_date": "2024-05-26T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Catalan GP",
"dates": "24 May - 26 May",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-05-25T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-05-26T12:00:00+00:00"
}
],
"last_session_end_time": "2024-05-26T14:30:00.000000Z"
},
{
"id": 494,
"shortname": "ITA",
"name": "Gran Premio d’Italia Brembo",
"hashtag": "#ItalianGP",
"circuit": "Autodromo Internazionale del Mugello",
"country_code": "IT",
"country": "Italy",
"start_date": "2024-05-31T00:00:00.000000Z",
"end_date": "2024-06-02T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Italian GP",
"dates": "31 May - 02 Jun",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-06-01T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-06-02T12:00:00+00:00"
}
],
"last_session_end_time": "2024-06-02T14:30:00.000000Z"
},
{
"id": 495,
"shortname": "IT1",
"name": "Mugello MotoGP™ Official Test ",
"hashtag": "#MugelloTest ",
"circuit": "Autodromo Internazionale del Mugello",
"country_code": "IT",
"country": "Italy",
"start_date": "2024-06-03T00:00:00.000000Z",
"end_date": "2024-06-03T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 1,
"has_timing": 1,
"friendly_name": "Mugello Test ",
"dates": "03 Jun",
"key_session_times": [],
"last_session_end_time": "2024-06-03T16:00:00.000000Z"
},
{
"id": 497,
"shortname": "NED",
"name": "Motul TT Assen",
"hashtag": "#DutchGP",
"circuit": "TT Circuit Assen",
"country_code": "NL",
"country": "Netherlands",
"start_date": "2024-06-28T00:00:00.000000Z",
"end_date": "2024-06-30T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Dutch GP",
"dates": "28 Jun - 30 Jun",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-06-29T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-06-30T12:00:00+00:00"
}
],
"last_session_end_time": "2024-06-30T14:30:00.000000Z"
},
{
"id": 498,
"shortname": "GER",
"name": "Liqui Moly Motorrad Grand Prix Deutschland",
"hashtag": "#GermanGP",
"circuit": "Sachsenring",
"country_code": "DE",
"country": "Germany",
"start_date": "2024-07-05T00:00:00.000000Z",
"end_date": "2024-07-07T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "German GP",
"dates": "05 Jul - 07 Jul",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-07-06T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-07-07T12:00:00+00:00"
}
],
"last_session_end_time": "2024-07-07T14:30:00.000000Z"
},
{
"id": 499,
"shortname": "GBR",
"name": "Monster Energy British Grand Prix",
"hashtag": "#BritishGP",
"circuit": "Silverstone Circuit",
"country_code": "GB",
"country": "United Kingdom",
"start_date": "2024-08-02T00:00:00.000000Z",
"end_date": "2024-08-04T00:00:00.000000Z",
"local_tz_offset": 1,
"test": 0,
"has_timing": 1,
"friendly_name": "British GP",
"dates": "02 Aug - 04 Aug",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-08-03T14:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-08-04T12:00:00+00:00"
}
],
"last_session_end_time": "2024-08-04T14:45:00.000000Z"
},
{
"id": 500,
"shortname": "AUT",
"name": "Motorrad Grand Prix von Österreich",
"hashtag": "#AustrianGP",
"circuit": "Red Bull Ring - Spielberg",
"country_code": "AT",
"country": "Austria",
"start_date": "2024-08-16T00:00:00.000000Z",
"end_date": "2024-08-18T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Austrian GP",
"dates": "16 Aug - 18 Aug",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-08-17T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-08-18T11:40:00+00:00"
}
],
"last_session_end_time": "2024-08-18T14:30:00.000000Z"
},
{
"id": 513,
"shortname": "AT2",
"name": "Spielberg Moto2™ & Moto3™ Official Test",
"hashtag": "#SpielbergTest",
"circuit": "Red Bull Ring - Spielberg",
"country_code": "AT",
"country": "Austria",
"start_date": "2024-08-19T00:00:00.000000Z",
"end_date": "2024-08-19T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 1,
"has_timing": 1,
"friendly_name": "Spielberg Test",
"dates": "19 Aug",
"key_session_times": [],
"last_session_end_time": "2024-08-19T16:00:00.000000Z"
},
{
"id": 501,
"shortname": "ARA",
"name": "Gran Premio GoPro de Aragón",
"hashtag": "#AragonGP",
"circuit": "MotorLand Aragón",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-08-30T00:00:00.000000Z",
"end_date": "2024-09-01T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "Aragon GP",
"dates": "30 Aug - 01 Sep",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-08-31T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-09-01T12:00:00+00:00"
}
],
"last_session_end_time": "2024-09-01T14:30:00.000000Z"
},
{
"id": 502,
"shortname": "RSM",
"name": "Gran Premio Red Bull di San Marino e della Riviera di Rimini",
"hashtag": "#SanMarinoGP",
"circuit": "Misano World Circuit Marco Simoncelli",
"country_code": "SM",
"country": "Italy",
"start_date": "2024-09-06T00:00:00.000000Z",
"end_date": "2024-09-08T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 1,
"friendly_name": "San Marino GP",
"dates": "06 Sep - 08 Sep",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-09-07T13:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-09-08T12:00:00+00:00"
}
],
"last_session_end_time": "2024-09-08T14:30:00.000000Z"
},
{
"id": 503,
"shortname": "IT2",
"name": "Misano MotoGP™ Official Test",
"hashtag": "#MisanoTest",
"circuit": "Misano World Circuit Marco Simoncelli",
"country_code": "IT",
"country": "Italy",
"start_date": "2024-09-09T00:00:00.000000Z",
"end_date": "2024-09-09T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 1,
"has_timing": 0,
"friendly_name": "Misano Test",
"dates": "09 Sep",
"key_session_times": [],
"last_session_end_time": null
},
{
"id": 504,
"shortname": "EMI",
"name": "Emilia-Romagna Grand Prix ",
"hashtag": "#EmiliaRomagnaGP",
"circuit": "Misano World Circuit Marco Simoncelli",
"country_code": "IT",
"country": "Italy",
"start_date": "2024-09-20T00:00:00.000000Z",
"end_date": "2024-09-22T00:00:00.000000Z",
"local_tz_offset": 2,
"test": 0,
"has_timing": 0,
"friendly_name": "Emilia Romagna GP",
"dates": "20 Sep - 22 Sep",
"key_session_times": [],
"last_session_end_time": null
},
{
"id": 505,
"shortname": "INA",
"name": "Pertamina Grand Prix of Indonesia",
"hashtag": "#IndonesianGP",
"circuit": "Pertamina Mandalika Circuit",
"country_code": "ID",
"country": "Indonesia",
"start_date": "2024-09-27T00:00:00.000000Z",
"end_date": "2024-09-29T00:00:00.000000Z",
"local_tz_offset": 8,
"test": 0,
"has_timing": 1,
"friendly_name": "Indonesian GP",
"dates": "27 Sep - 29 Sep",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-09-28T07:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-09-29T07:00:00+00:00"
}
],
"last_session_end_time": "2024-09-29T09:30:00.000000Z"
},
{
"id": 506,
"shortname": "JPN",
"name": "Motul Grand Prix of Japan",
"hashtag": "#JapaneseGP",
"circuit": "Mobility Resort Motegi",
"country_code": "JP",
"country": "Japan",
"start_date": "2024-10-04T00:00:00.000000Z",
"end_date": "2024-10-06T00:00:00.000000Z",
"local_tz_offset": 9,
"test": 0,
"has_timing": 1,
"friendly_name": "Japanese GP",
"dates": "04 Oct - 06 Oct",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-10-05T06:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-10-06T05:00:00+00:00"
}
],
"last_session_end_time": "2024-10-06T07:30:00.000000Z"
},
{
"id": 507,
"shortname": "AUS",
"name": "Qatar Airways Australian Motorcycle Grand Prix",
"hashtag": "#AustralianGP",
"circuit": "Phillip Island",
"country_code": "AU",
"country": "Australia",
"start_date": "2024-10-18T00:00:00.000000Z",
"end_date": "2024-10-20T00:00:00.000000Z",
"local_tz_offset": 11,
"test": 0,
"has_timing": 1,
"friendly_name": "Australian GP",
"dates": "18 Oct - 20 Oct",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-10-19T04:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-10-20T03:00:00+00:00"
}
],
"last_session_end_time": "2024-10-20T05:30:00.000000Z"
},
{
"id": 508,
"shortname": "THA",
"name": "PT Grand Prix of Thailand",
"hashtag": "#ThaiGP",
"circuit": "Chang International Circuit",
"country_code": "TH",
"country": "Thailand",
"start_date": "2024-10-25T00:00:00.000000Z",
"end_date": "2024-10-27T00:00:00.000000Z",
"local_tz_offset": 7,
"test": 0,
"has_timing": 1,
"friendly_name": "Thai GP",
"dates": "25 Oct - 27 Oct",
"key_session_times": [
{
"session_shortname": "SPR",
"session_name": "Tissot Sprint",
"start_datetime_utc": "2024-10-26T08:00:00+00:00"
},
{
"session_shortname": "RAC",
"session_name": "Race",
"start_datetime_utc": "2024-10-27T08:00:00+00:00"
}
],
"last_session_end_time": "2024-10-27T10:30:00.000000Z"
},
{
"id": 509,
"shortname": "MAL",
"name": "PETRONAS Grand Prix of Malaysia",
"hashtag": "#MalaysianGP",
"circuit": "Petronas Sepang International Circuit",
"country_code": "MY",
"country": "Malaysia",
"start_date": "2024-11-01T00:00:00.000000Z",
"end_date": "2024-11-03T00:00:00.000000Z",
"local_tz_offset": 8,
"test": 0,
"has_timing": 1,
"friendly_name": "Malaysian GP",
"dates": "01 Nov - 03 Nov",
"key_session_times": [2 items],
"last_session_end_time": "2024-11-03T09:30:00.000000Z"
},
{
"id": 510,
"shortname": "VAL",
"name": "Gran Premio Motul de la Comunitat Valenciana",
"hashtag": "#ValenciaGP",
"circuit": "Circuit Ricardo Tormo",
"country_code": "ES",
"country": "Spain",
"start_date": "2024-11-15T00:00:00.000000Z",
"end_date": "2024-11-17T00:00:00.000000Z",
"local_tz_offset": 1,
"test": 0,
"has_timing": 1,
"friendly_name": "Valencia GP",
"dates": "15 Nov - 17 Nov",
"key_session_times": [2 items],
"last_session_end_time": "2024-11-17T15:30:00.000000Z"
}
]
}
"""

data = json.loads(json_data)

try:
	with cnx.cursor() as cursor:

		# Inserisci i dati
		insert_main_data(cursor, data)
		
		# Inserisci i dati delle sessioni chiave
		for event in data['calendar']:
			if event['key_session_times']:
				insert_session_data(cursor, event['id'], event['key_session_times'])
	
	# Conferma le modifiche al database
	cnx.commit()

finally:
	cnx.close()

print("Dati importati con successo!")
