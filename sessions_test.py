import logging

from modules.runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


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
                  AND test = 1
                  AND date_start BETWEEN CURDATE() - INTERVAL 150 DAY AND CURDATE() + INTERVAL 30 DAY
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
                        logging.error("Errore fetch session test event=%s category=%s: %s", event_id, category_id, exc)
                        continue

                    for item in sessions:
                        session_files = item.get("session_files", {})
                        condition = item.get("condition", {})
                        category_info = item.get("category", {})
                        event_info = item.get("event", {})
                        event_circuit = event_info.get("circuit", {})
                        event_country = event_info.get("country", {})

                        cursor.execute(
                            """
                            INSERT INTO sessions (
                                id, date, number, track_condition, air_condition, humidity_condition, ground_condition,
                                weather_condition, circuit_name, classification_url, analysis_url, average_speed_url,
                                fast_lap_sequence_url, lap_chart_url, analysis_by_lap_url, fast_lap_rider_url, grid_url,
                                session_url, world_standing_url, best_partial_time_url, maximum_speed_url,
                                combined_practice_url, combined_classification_url, type, category_id,
                                category_legacy_id, category_name, event_id, event_name, event_sponsored_name, year,
                                circuit_id, circuit_legacy_id, circuit_place, circuit_nation, country_iso, country_name,
                                country_region_iso, event_short_name, status
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON DUPLICATE KEY UPDATE
                                status = VALUES(status),
                                date = VALUES(date)
                            """,
                            (
                                item.get("id"),
                                item.get("date"),
                                item.get("number"),
                                condition.get("track"),
                                condition.get("air"),
                                condition.get("humidity"),
                                condition.get("ground"),
                                condition.get("weather"),
                                item.get("circuit"),
                                (session_files.get("classification") or {}).get("url"),
                                (session_files.get("analysis") or {}).get("url"),
                                (session_files.get("average_speed") or {}).get("url"),
                                (session_files.get("fast_lap_sequence") or {}).get("url"),
                                (session_files.get("lap_chart") or {}).get("url"),
                                (session_files.get("analysis_by_lap") or {}).get("url"),
                                (session_files.get("fast_lap_rider") or {}).get("url"),
                                (session_files.get("grid") or {}).get("url"),
                                (session_files.get("session") or {}).get("url"),
                                (session_files.get("world_standing") or {}).get("url"),
                                (session_files.get("best_partial_time") or {}).get("url"),
                                (session_files.get("maximum_speed") or {}).get("url"),
                                (session_files.get("combined_practice") or {}).get("url"),
                                (session_files.get("combined_classification") or {}).get("url"),
                                item.get("type"),
                                category_info.get("id"),
                                category_info.get("legacy_id"),
                                category_info.get("name"),
                                event_info.get("id"),
                                event_info.get("name"),
                                event_info.get("sponsored_name"),
                                event_info.get("season"),
                                event_circuit.get("id"),
                                event_circuit.get("legacy_id"),
                                event_circuit.get("place"),
                                event_circuit.get("nation"),
                                event_country.get("iso"),
                                event_country.get("name"),
                                event_country.get("region_iso"),
                                event_info.get("short_name"),
                                item.get("status"),
                            ),
                        )
                        total += 1

        cnx.commit()

    logging.info("Sessioni test elaborate anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
