import requests

import os
from dotenv import load_dotenv

load_dotenv()

import pymysql
import os

import pymysql
import os
import requests
import json


def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            passwd=os.getenv('DB_PASSWD'),
            db=os.getenv('DB_NAME'),
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
            charset=os.getenv('DB_CHARSET')
        )
        print("Connected to the MySQL database.")
        return connection
    except Exception as e:
        print("Error connecting to the MySQL database:", e)
        return None


# Function to fetch data from the live timing API
def fetch_live_timing_data():
    url = "https://api.motogp.pulselive.com/motogp/v1/timing-gateway/livetiming-lite"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            # Check if the JSON data contains the required keys and items
            if "head" in json_data and "rider" in json_data \
                and len(json_data["head"]) >= 5 and len(json_data["rider"]) >= 5:
                return json_data
            else:
                print("Invalid JSON data format.")
                return None
        else:
            print("Failed to fetch data. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", e)
        return None



# Function to insert the "head" section data into the MySQL database
def insert_head_data_to_mysql(connection, head_data):
    try:
        with connection.cursor() as cursor:
            # Construct the insert query for the head data
            
            sql_truncate = "TRUNCATE TABLE live_results_head"
            cursor.execute(sql_truncate)
            connection.commit()
            sql_truncate = "TRUNCATE TABLE live_results"
            cursor.execute(sql_truncate)
            connection.commit()
            
            
            
            sql = """INSERT INTO live_results_head (championship_id, category, circuit_id, circuit_name, event_id, event_tv_name,
                     date, session_id, session_type, session_name, duration, remaining, session_status_id, session_status_name,
                     date_formated, url, trsid)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            # Execute the insert query for the head data
            cursor.execute(sql,
                           (head_data.get('championship_id'), head_data.get('category'), head_data.get('circuit_id'),
                            head_data.get('circuit_name'), head_data.get('event_id'), head_data.get('event_tv_name'),
                            head_data.get('date'), head_data.get('session_id'), head_data.get('session_type'),
                            head_data.get('session_name'), head_data.get('duration'), head_data.get('remaining'),
                            head_data.get('session_status_id'), head_data.get('session_status_name'),
                            head_data.get('date_formated'), head_data.get('url'), head_data.get('trsid')))
            
            # Commit the changes
            connection.commit()
            
            print("Head data inserted into the MySQL database successfully.")
    except Exception as e:
        print("Error inserting head data into MySQL database:", e)


# Function to insert rider data into the MySQL database
def insert_rider_data_to_mysql(connection, rider_data):
    try:
        with connection.cursor() as cursor:
            
            
            
            # Construct the insert query for the rider data
            sql = """INSERT INTO live_results (rider_id, pos, rider_number, rider_name, rider_surname, team_name,
                     status_id, status_name, lap_time, num_lap, last_lap, last_lap_time, gap_first, gap_prev,
                     trac_status, rider_url, on_pit)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            
            # Execute the insert query for the rider data
            cursor.execute(sql, (rider_data.get('rider_id'), rider_data.get('pos'), rider_data.get('rider_number'),
                                 rider_data.get('rider_name'), rider_data.get('rider_surname'),
                                 rider_data.get('team_name'),
                                 rider_data.get('status_id'), rider_data.get('status_name'), rider_data.get('lap_time'),
                                 rider_data.get('num_lap'), rider_data.get('last_lap'), rider_data.get('last_lap_time'),
                                 rider_data.get('gap_first'), rider_data.get('gap_prev'), rider_data.get('trac_status'),
                                 rider_data.get('rider_url'), rider_data.get('on_pit')))
            
            # Commit the changes
            connection.commit()
            
            print(
                f"Rider data for {rider_data.get('rider_name')} {rider_data.get('rider_surname')} inserted into the MySQL database successfully.")
    except Exception as e:
        print("Error inserting rider data into MySQL database:", e)


# Main function
def main():
    # Connect to the MySQL database
    connection = connect_to_mysql()
    if connection:
        # Fetch data from the live timing API
        live_timing_data = fetch_live_timing_data()
        if live_timing_data:
            # Insert the "head" section data into the MySQL database
            head_data = live_timing_data.get('head')
            if head_data:
                print("Head data found in live timing data.")
                insert_head_data_to_mysql(connection, head_data)
            else:
                print("No head data found in live timing data.")
            
            # Extract rider data and insert into the MySQL database
            rider_data = live_timing_data.get('rider')
            if rider_data:
                for rider_id, rider_info in rider_data.items():
                    insert_rider_data_to_mysql(connection, rider_info)
            else:
                print("No rider data found in live timing data.")
        else:
            print("Failed to fetch live timing data.")
    else:
        print("Failed to connect to the MySQL database.")


if __name__ == "__main__":
    main()