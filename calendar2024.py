import json
import pymysql
import os
import requests
from datetime import datetime  # Importa datetime per la conversione delle date
from dotenv import load_dotenv

load_dotenv()

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


# Funzione per convertire le date nel formato corretto
def convert_date(date_string):
	if date_string:
		try:
			# Converti al formato 'YYYY-MM-DD HH:MM:SS'
			return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
		except ValueError:
			pass
	return None


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
			event['country_code'], event['country'],
			convert_date(event['start_date']),  # Converte la data
			convert_date(event['end_date']),  # Converte la data
			event['local_tz_offset'], event['test'], event['has_timing'], event['friendly_name'],
			event['dates'],
			convert_date(event['last_session_end_time'])  # Converte la data
		))


# Funzione per inserire le sessioni chiave nella tabella delle sessioni
def insert_session_data(cursor, event_id, sessions):
	insert_query = """
    INSERT INTO MotoGP_KeySessionTimes (event_id, session_shortname, session_name, start_datetime_utc)
    VALUES (%s, %s, %s, %s)
    """
	
	for session in sessions:
		cursor.execute(insert_query, (
			event_id,
			session['session_shortname'],
			session['session_name'],
			session['start_datetime_utc']  # Assicurati che questo valore sia presente
		))


# Effettua la richiesta all'API e ottieni il JSON
api_url = "https://mototiming.live/api/schedule?filter=all"
response = requests.get(api_url)

# Verifica se la richiesta è andata a buon fine
if response.status_code == 200:
	data = response.json()  # Carica il JSON dalla risposta
else:
	print(f"Errore durante il recupero dei dati dall'API: {response.status_code}")
	cnx.close()
	exit()

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
