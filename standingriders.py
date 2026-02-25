import hashlib
import logging

from modules.runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    total = 0
    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute("SELECT id, year FROM seasons WHERE year = %s", (target_year,))
            seasons = cursor.fetchall()

            for season_row in seasons:
                season_id = season_row["id"]
                year = season_row["year"]

                cursor.execute("SELECT id, name FROM categories_general WHERE year = %s", (year,))
                categories = cursor.fetchall()

                for category in categories:
                    category_id = category["id"]
                    url = (
                        "https://api.motogp.pulselive.com/motogp/v1/results/standings"
                        f"?seasonUuid={season_id}&categoryUuid={category_id}"
                    )
                    try:
                        data = request_json(session, url)
                    except Exception as exc:
                        logging.error("Errore standings season=%s category=%s: %s", season_id, category_id, exc)
                        continue

                    xml_file = data.get("xmlFile")
                    for item in data.get("classification", []):
                        rider = item.get("rider") or {}
                        constructor = item.get("constructor") or {}
                        team = item.get("team") or {}
                        team_season = team.get("season") or {}

                        digest = hashlib.md5(
                            f"{rider.get('id')}{team.get('id')}{constructor.get('id')}{year}{season_id}{category_id}".encode(
                                "utf-8"
                            )
                        ).hexdigest()

                        cursor.execute(
                            """
                            INSERT INTO standing_riders (
                                classification_id, position, rider_id, rider_full_name, rider_country_iso,
                                rider_country_name, rider_region_iso, rider_legacy_id, rider_number,
                                riders_api_uuid, team_id, team_name, team_legacy_id, season_id,
                                season_year, season_current, constructor_id, constructor_name,
                                constructor_legacy_id, session, points, xmlFile, year, md5,
                                session_id, category_id
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON DUPLICATE KEY UPDATE
                                position = VALUES(position),
                                rider_full_name = VALUES(rider_full_name),
                                rider_country_iso = VALUES(rider_country_iso),
                                rider_country_name = VALUES(rider_country_name),
                                rider_region_iso = VALUES(rider_region_iso),
                                rider_legacy_id = VALUES(rider_legacy_id),
                                rider_number = VALUES(rider_number),
                                riders_api_uuid = VALUES(riders_api_uuid),
                                team_id = VALUES(team_id),
                                team_name = VALUES(team_name),
                                team_legacy_id = VALUES(team_legacy_id),
                                season_id = VALUES(season_id),
                                season_year = VALUES(season_year),
                                season_current = VALUES(season_current),
                                constructor_id = VALUES(constructor_id),
                                constructor_name = VALUES(constructor_name),
                                constructor_legacy_id = VALUES(constructor_legacy_id),
                                session = VALUES(session),
                                points = VALUES(points),
                                xmlFile = VALUES(xmlFile),
                                year = VALUES(year),
                                md5 = VALUES(md5),
                                session_id = VALUES(session_id),
                                category_id = VALUES(category_id)
                            """,
                            (
                                item.get("id"),
                                item.get("position"),
                                rider.get("id"),
                                rider.get("full_name"),
                                (rider.get("country") or {}).get("iso"),
                                (rider.get("country") or {}).get("name"),
                                (rider.get("country") or {}).get("region_iso"),
                                rider.get("legacy_id"),
                                rider.get("number"),
                                rider.get("riders_api_uuid"),
                                team.get("id"),
                                team.get("name"),
                                team.get("legacy_id"),
                                team_season.get("id"),
                                team_season.get("year"),
                                team_season.get("current"),
                                constructor.get("id"),
                                constructor.get("name"),
                                constructor.get("legacy_id"),
                                item.get("session"),
                                item.get("points"),
                                xml_file,
                                year,
                                digest,
                                season_id,
                                category_id,
                            ),
                        )
                        total += 1

        cnx.commit()

    logging.info("Standing riders elaborati anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
