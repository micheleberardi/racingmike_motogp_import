import logging

from runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging

SESSIONS_FIELDS = [
    "id",
    "date",
    "number",
    "track_condition",
    "air_condition",
    "humidity_condition",
    "ground_condition",
    "weather_condition",
    "circuit_name",
    "classification_url",
    "classification_menu_position",
    "analysis_url",
    "analysis_menu_position",
    "average_speed_url",
    "average_speed_menu_position",
    "fast_lap_sequence_url",
    "fast_lap_sequence_menu_position",
    "lap_chart_url",
    "lap_chart_menu_position",
    "analysis_by_lap_url",
    "analysis_by_lap_menu_position",
    "fast_lap_rider_url",
    "fast_lap_rider_menu_position",
    "grid_url",
    "grid_menu_position",
    "session_url",
    "session_menu_position",
    "world_standing_url",
    "world_standing_menu_position",
    "best_partial_time_url",
    "best_partial_time_menu_position",
    "maximum_speed_url",
    "maximum_speed_menu_position",
    "combined_practice_url",
    "combined_practice_menu_position",
    "combined_classification_url",
    "combined_classification_menu_position",
    "type",
    "category_id",
    "category_legacy_id",
    "category_name",
    "event_id",
    "event_name",
    "event_sponsored_name",
    "year",
    "circuit_id",
    "circuit_legacy_id",
    "circuit_place",
    "circuit_nation",
    "country_iso",
    "country_name",
    "country_region_iso",
    "event_short_name",
    "status",
]

_COLS = ", ".join(SESSIONS_FIELDS)
_PLACEHOLDERS = ", ".join(["%s"] * len(SESSIONS_FIELDS))
_UPDATE = ",\n    ".join(
    f"{f} = VALUES({f})"
    for f in SESSIONS_FIELDS
    if f != "id"
)

SESSIONS_QUERY = f"""
INSERT INTO sessions ({_COLS})
VALUES ({_PLACEHOLDERS})
ON DUPLICATE KEY UPDATE
    {_UPDATE}
"""


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    total = 0
    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, year
                FROM events
                WHERE year = %s
                  AND test != 1
                  AND date_start BETWEEN CURDATE() - INTERVAL 60 DAY AND CURDATE() + INTERVAL 30 DAY
                ORDER BY date_start DESC
                """,
                (target_year,),
            )
            events = cursor.fetchall()

            for event in events:
                event_id = event["id"]
                year = event["year"]
                cursor.execute("SELECT id FROM categories_general WHERE year = %s", (year,))
                categories = cursor.fetchall()

                for category in categories:
                    category_id = category["id"]
                    url = (
                        "https://api.motogp.pulselive.com/motogp/v1/results/sessions"
                        f"?eventUuid={event_id}&categoryUuid={category_id}"
                    )
                    try:
                        sessions = request_json(session, url)
                    except Exception as exc:
                        logging.error("Errore fetch sessions event=%s category=%s: %s", event_id, category_id, exc)
                        continue

                    for item in sessions:
                        sf = item.get("session_files") or {}
                        cond = item.get("condition") or {}
                        cat = item.get("category") or {}
                        ev = item.get("event") or {}
                        circuit = ev.get("circuit") or {}
                        country = ev.get("country") or {}

                        def _file(key, field):
                            return (sf.get(key) or {}).get(field)

                        payload = {
                            "id": item.get("id"),
                            "date": item.get("date"),
                            "number": item.get("number"),
                            "track_condition": cond.get("track"),
                            "air_condition": cond.get("air"),
                            "humidity_condition": cond.get("humidity"),
                            "ground_condition": cond.get("ground"),
                            "weather_condition": cond.get("weather"),
                            "circuit_name": item.get("circuit"),
                            "classification_url": _file("classification", "url"),
                            "classification_menu_position": _file("classification", "menu_position"),
                            "analysis_url": _file("analysis", "url"),
                            "analysis_menu_position": _file("analysis", "menu_position"),
                            "average_speed_url": _file("average_speed", "url"),
                            "average_speed_menu_position": _file("average_speed", "menu_position"),
                            "fast_lap_sequence_url": _file("fast_lap_sequence", "url"),
                            "fast_lap_sequence_menu_position": _file("fast_lap_sequence", "menu_position"),
                            "lap_chart_url": _file("lap_chart", "url"),
                            "lap_chart_menu_position": _file("lap_chart", "menu_position"),
                            "analysis_by_lap_url": _file("analysis_by_lap", "url"),
                            "analysis_by_lap_menu_position": _file("analysis_by_lap", "menu_position"),
                            "fast_lap_rider_url": _file("fast_lap_rider", "url"),
                            "fast_lap_rider_menu_position": _file("fast_lap_rider", "menu_position"),
                            "grid_url": _file("grid", "url"),
                            "grid_menu_position": _file("grid", "menu_position"),
                            "session_url": _file("session", "url"),
                            "session_menu_position": _file("session", "menu_position"),
                            "world_standing_url": _file("world_standing", "url"),
                            "world_standing_menu_position": _file("world_standing", "menu_position"),
                            "best_partial_time_url": _file("best_partial_time", "url"),
                            "best_partial_time_menu_position": _file("best_partial_time", "menu_position"),
                            "maximum_speed_url": _file("maximum_speed", "url"),
                            "maximum_speed_menu_position": _file("maximum_speed", "menu_position"),
                            "combined_practice_url": _file("combined_practice", "url"),
                            "combined_practice_menu_position": _file("combined_practice", "menu_position"),
                            "combined_classification_url": _file("combined_classification", "url"),
                            "combined_classification_menu_position": _file("combined_classification", "menu_position"),
                            "type": item.get("type"),
                            "category_id": cat.get("id"),
                            "category_legacy_id": cat.get("legacy_id"),
                            "category_name": cat.get("name"),
                            "event_id": ev.get("id"),
                            "event_name": ev.get("name"),
                            "event_sponsored_name": ev.get("sponsored_name"),
                            "year": ev.get("season"),
                            "circuit_id": circuit.get("id"),
                            "circuit_legacy_id": circuit.get("legacy_id"),
                            "circuit_place": circuit.get("place"),
                            "circuit_nation": circuit.get("nation"),
                            "country_iso": country.get("iso"),
                            "country_name": country.get("name"),
                            "country_region_iso": country.get("region_iso"),
                            "event_short_name": ev.get("short_name"),
                            "status": item.get("status"),
                        }

                        cursor.execute(
                            SESSIONS_QUERY,
                            tuple(payload[f] for f in SESSIONS_FIELDS),
                        )
                        total += 1

        cnx.commit()

    logging.info("Sessioni elaborate anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
