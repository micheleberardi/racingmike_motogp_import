import logging

from modules.runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    total_events = 0
    total_legacy = 0

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute("SELECT id, year FROM seasons WHERE year = %s", (target_year,))
            seasons = cursor.fetchall()

            for season in seasons:
                season_id = season["id"]
                year = season["year"]
                url = f"https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid={season_id}&isFinished=true"
                try:
                    events = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore fetch eventi season %s: %s", season_id, exc)
                    continue

                for event in events:
                    event_files = event.get("event_files", {})
                    circuit_info = event_files.get("circuit_information", {})
                    podiums = event_files.get("podiums", {})
                    pole_positions = event_files.get("pole_positions", {})
                    nations_statistics = event_files.get("nations_statistics", {})
                    riders_all_time = event_files.get("riders_all_time", {})
                    circuit = event.get("circuit", {})
                    season_info = event.get("season", {})
                    country = event.get("country", {})

                    cursor.execute(
                        """
                        INSERT INTO events (
                            id, country_iso, country_name, country_region_iso,
                            event_circuit_information_url, event_circuit_information_menu_position,
                            event_podiums_url, event_podiums_menu_position,
                            event_pole_positions_url, event_pole_positions_menu_position,
                            event_nations_statistics_url, event_nations_statistics_menu_position,
                            event_riders_all_time_url, event_riders_all_time_menu_position,
                            circuit_id, circuit_name, circuit_legacy_id,
                            circuit_place, circuit_nation, test, sponsored_name,
                            date_end, toad_api_uuid, date_start, name,
                            season_id, year, season_current, short_name
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            date_start = VALUES(date_start),
                            date_end = VALUES(date_end),
                            sponsored_name = VALUES(sponsored_name),
                            short_name = VALUES(short_name)
                        """,
                        (
                            event.get("id"),
                            country.get("iso"),
                            country.get("name"),
                            country.get("region_iso"),
                            circuit_info.get("url"),
                            circuit_info.get("menu_position"),
                            podiums.get("url"),
                            podiums.get("menu_position"),
                            pole_positions.get("url"),
                            pole_positions.get("menu_position"),
                            nations_statistics.get("url"),
                            nations_statistics.get("menu_position"),
                            riders_all_time.get("url"),
                            riders_all_time.get("menu_position"),
                            circuit.get("id"),
                            circuit.get("name"),
                            circuit.get("legacy_id"),
                            circuit.get("place"),
                            circuit.get("nation"),
                            event.get("test"),
                            event.get("sponsored_name"),
                            event.get("date_end"),
                            event.get("toad_api_uuid"),
                            event.get("date_start"),
                            event.get("name"),
                            season_info.get("id"),
                            season_info.get("year", year),
                            season_info.get("current"),
                            event.get("short_name"),
                        ),
                    )
                    total_events += 1

                    seen_legacy = set()
                    for legacy in event.get("legacy_id", []) or []:
                        legacy_key = (
                            event.get("id"),
                            legacy.get("categoryId"),
                            legacy.get("eventId"),
                        )
                        if legacy_key in seen_legacy:
                            continue
                        seen_legacy.add(legacy_key)
                        cursor.execute(
                            """
                            INSERT INTO event_legacy_ids (event_id, categoryId, eventId)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE eventId = VALUES(eventId)
                            """,
                            (event.get("id"), legacy.get("categoryId"), legacy.get("eventId")),
                        )
                        total_legacy += 1

        cnx.commit()

    logging.info("Eventi elaborati anno %s: %s, legacy ids: %s", target_year, total_events, total_legacy)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
