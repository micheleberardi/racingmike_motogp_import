import logging

from runtime import get_db_connection, get_http_session, request_json, setup_logging

LIVE_URL = "https://api.motogp.pulselive.com/motogp/v1/timing-gateway/livetiming-lite"


def main() -> int:
    setup_logging()
    session = get_http_session()

    try:
        payload = request_json(session, LIVE_URL)
    except Exception as exc:
        logging.error("Errore fetch live timing: %s", exc)
        return 1

    head_data = payload.get("head")
    riders = payload.get("rider") or {}

    if not head_data or not isinstance(riders, dict):
        logging.error("Formato payload live non valido")
        return 1

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE live_results_head")
            cursor.execute("TRUNCATE TABLE live_results")

            cursor.execute(
                """
                INSERT INTO live_results_head (
                    championship_id, category, circuit_id, circuit_name, event_id, event_tv_name,
                    date, session_id, session_type, session_name, duration, remaining,
                    session_status_id, session_status_name, date_formated, url, trsid
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    head_data.get("championship_id"),
                    head_data.get("category"),
                    head_data.get("circuit_id"),
                    head_data.get("circuit_name"),
                    head_data.get("event_id"),
                    head_data.get("event_tv_name"),
                    head_data.get("date"),
                    head_data.get("session_id"),
                    head_data.get("session_type"),
                    head_data.get("session_name"),
                    head_data.get("duration"),
                    head_data.get("remaining"),
                    head_data.get("session_status_id"),
                    head_data.get("session_status_name"),
                    head_data.get("date_formated"),
                    head_data.get("url"),
                    head_data.get("trsid"),
                ),
            )

            for rider_info in riders.values():
                cursor.execute(
                    """
                    INSERT INTO live_results (
                        rider_id, pos, rider_number, rider_name, rider_surname, team_name,
                        status_id, status_name, lap_time, num_lap, last_lap, last_lap_time,
                        gap_first, gap_prev, trac_status, rider_url, on_pit
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rider_info.get("rider_id"),
                        rider_info.get("pos"),
                        rider_info.get("rider_number"),
                        rider_info.get("rider_name"),
                        rider_info.get("rider_surname"),
                        rider_info.get("team_name"),
                        rider_info.get("status_id"),
                        rider_info.get("status_name"),
                        rider_info.get("lap_time"),
                        rider_info.get("num_lap"),
                        rider_info.get("last_lap"),
                        rider_info.get("last_lap_time"),
                        rider_info.get("gap_first"),
                        rider_info.get("gap_prev"),
                        rider_info.get("trac_status"),
                        rider_info.get("rider_url"),
                        rider_info.get("on_pit"),
                    ),
                )

        cnx.commit()

    logging.info("Live timing import completato: %s rider", len(riders))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
