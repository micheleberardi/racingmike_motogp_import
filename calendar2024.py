import logging
from datetime import datetime

from modules.runtime import get_db_connection, get_http_session, request_json, setup_logging

API_URL = "https://mototiming.live/api/schedule?filter=all"


def convert_date(date_string):
    if not date_string:
        return None
    for pattern in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S+00:00"):
        try:
            return datetime.strptime(date_string, pattern).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    logging.error("Errore conversione data: %s", date_string)
    return None


def main() -> int:
    setup_logging()
    session = get_http_session()

    try:
        data = request_json(session, API_URL)
    except Exception as exc:
        logging.error("Errore recupero calendario: %s", exc)
        return 1

    calendar = data.get("calendar") or []

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            for event in calendar:
                cursor.execute(
                    """
                    INSERT INTO MotoGP_Calendar (
                        id, shortname, name, hashtag, circuit, country_code, country,
                        start_date, end_date, local_tz_offset, test, has_timing,
                        friendly_name, dates, last_session_end_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        last_session_end_time = VALUES(last_session_end_time)
                    """,
                    (
                        event.get("id"),
                        event.get("shortname"),
                        event.get("name"),
                        event.get("hashtag"),
                        event.get("circuit"),
                        event.get("country_code"),
                        event.get("country"),
                        convert_date(event.get("start_date")),
                        convert_date(event.get("end_date")),
                        event.get("local_tz_offset"),
                        event.get("test"),
                        event.get("has_timing"),
                        event.get("friendly_name"),
                        event.get("dates"),
                        convert_date(event.get("last_session_end_time")),
                    ),
                )

                for key_session in event.get("key_session_times") or []:
                    cursor.execute(
                        """
                        INSERT INTO MotoGP_KeySessionTimes (
                            event_id, session_shortname, session_name, start_datetime_utc
                        ) VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            session_shortname = VALUES(session_shortname),
                            session_name = VALUES(session_name),
                            start_datetime_utc = VALUES(start_datetime_utc)
                        """,
                        (
                            event.get("id"),
                            key_session.get("session_shortname"),
                            key_session.get("session_name"),
                            convert_date(key_session.get("start_datetime_utc")),
                        ),
                    )

        cnx.commit()

    logging.info("Calendario importato: %s eventi", len(calendar))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
