import json
import pymysql
import os
import requests
from datetime import datetime  # Importa datetime per la conversione delle date
from dotenv import load_dotenv
import logging

# Configura il logging
logging.basicConfig(level=logging.INFO)
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


# Funzione per convertire le date nel formato DATETIME
def convert_date(date_string):
	if date_string:
		try:
			# Converte la stringa ISO 8601 in oggetto datetime
			date_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
			return date_obj.strftime('%Y-%m-%d %H:%M:%S')
		except ValueError:
			try:
				# Gestisce il formato senza microsecondi
				date_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S+00:00')
				return date_obj.strftime('%Y-%m-%d %H:%M:%S')
			except ValueError:
				logging.error(f"Errore nella conversione della data: {date_string}")
				return None
	return None


# Funzione per inserire i dati nella tabella principale
def insert_main_data(cursor, data):
	insert_query = """
    INSERT INTO MotoGP_Calendar (id, shortname, name, hashtag, circuit, country_code, country,
        start_date, end_date, local_tz_offset, test, has_timing, friendly_name, dates, last_session_end_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        shortname = VALUES(shortname),
        name = VALUES(name),
        hashtag = VALUES(hashtag),
        circuit = VALUES(circuit),
        country_code = VALUES(country_code),
        country = VALUES(country),
        start_date = VALUES(start_date),
        end_date = VALUES(end_date),
        local_tz_offset = VALUES(local_tz_offset),
        test = VALUES(test),
        has_timing = VALUES(has_timing),
        friendly_name = VALUES(friendly_name),
        dates = VALUES(dates),
        last_session_end_time = VALUES(last_session_end_time);
    """
	
	for event in data['calendar']:
		logging.info(f"Elaborazione dell'evento ID: {event['id']} - {event['name']}")
		try:
			cursor.execute(insert_query, (
				event['id'], event['shortname'], event['name'], event['hashtag'], event['circuit'],
				event['country_code'], event['country'],
				convert_date(event['start_date']),  # Converte la data
				convert_date(event['end_date']),  # Converte la data
				event['local_tz_offset'], event['test'], event['has_timing'], event['friendly_name'],
				event['dates'],
				convert_date(event['last_session_end_time'])  # Converte la data
			))
		except Exception as e:
			logging.error(f"Errore durante l'inserimento dell'evento ID {event['id']}: {e}")


# Funzione per inserire le sessioni chiave nella tabella delle sessioni
def insert_session_data(cursor, event_id, sessions):
	insert_query = """
    INSERT INTO MotoGP_KeySessionTimes (event_id, session_shortname, session_name, start_datetime_utc)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        session_shortname = VALUES(session_shortname),
        session_name = VALUES(session_name),
        start_datetime_utc = VALUES(start_datetime_utc);
    """
	
	for session in sessions:
		formatted_date = convert_date(session['start_datetime_utc'])  # Converte la data
		logging.info(
			f"Inserimento dati: {event_id}, {session['session_shortname']}, {session['session_name']}, {formatted_date}")
		try:
			cursor.execute(insert_query, (
				event_id,
				session['session_shortname'],
				session['session_name'],
				formatted_date
			))
		except pymysql.err.IntegrityError as e:
			if 'foreign key constraint fails' in str(e):
				logging.error(f"L'evento con ID {event_id} non esiste in MotoGP_Calendar. Skipping.")
			else:
				logging.error(f"Errore durante l'inserimento della sessione per l'evento ID {event_id}: {e}")
		except Exception as e:
			logging.error(f"Errore durante l'inserimento della sessione per l'evento ID {event_id}: {e}")


# Effettua la richiesta all'API e ottieni il JSON
api_url = "https://mototiming.live/api/schedule?filter=all"
response = requests.get(api_url)

# Verifica se la richiesta è andata a buon fine
if response.status_code == 200:
	data = response.json()  # Carica il JSON dalla risposta
else:
	logging.error(f"Errore durante il recupero dei dati dall'API: {response.status_code}")
	cnx.close()
	exit()

try:
	with cnx.cursor() as cursor:
		logging.info("Inizio inserimento dati nella tabella MotoGP_Calendar.")
		insert_main_data(cursor, data)
		
		# Inserisci i dati delle sessioni chiave
		for event in data['calendar']:
			if event['key_session_times']:
				insert_session_data(cursor, event['id'], event['key_session_times'])
		
		# Conferma le modifiche al database
		cnx.commit()
		logging.info("Dati inseriti e commit effettuato con successo.")

except pymysql.err.IntegrityError as e:
	logging.error(f"Errore di integrità: {e}")
	cnx.rollback()  # Annulla le modifiche se c'è un errore

except Exception as e:
	logging.error(f"Errore durante l'esecuzione della query: {e}")
	cnx.rollback()  # Annulla le modifiche se c'è un errore

finally:
	cnx.close()
	logging.info("Connessione al database chiusa.")

print("Dati importati con successo!")
