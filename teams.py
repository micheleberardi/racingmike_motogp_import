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
            cursor.execute("SELECT id, year FROM categories_by_season WHERE year = %s", (target_year,))
            categories = cursor.fetchall()

            for category in categories:
                category_id = category["id"]
                year = category["year"]
                url = (
                    "https://api.motogp.pulselive.com/motogp/v1/teams"
                    f"?categoryUuid={category_id}&seasonYear={year}"
                )
                try:
                    teams = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore teams category=%s year=%s: %s", category_id, year, exc)
                    continue

                for team in teams:
                    constructor = team.get("constructor") or {}
                    for rider in team.get("riders", []) or []:
                        current_step = rider.get("current_career_step") or {}
                        rider_team = current_step.get("team") or {}
                        rider_category = current_step.get("category") or {}
                        pictures = current_step.get("pictures") or {}

                        md5_hash = hashlib.md5(f"{year}{rider.get('legacy_id')}".encode("utf-8")).hexdigest()
                        values = (
                            team.get("id"),
                            team.get("name"),
                            team.get("legacy_id"),
                            team.get("color"),
                            team.get("text_color"),
                            team.get("picture"),
                            constructor.get("id"),
                            constructor.get("name"),
                            constructor.get("legacy_id"),
                            rider.get("id"),
                            rider.get("name"),
                            rider.get("surname"),
                            rider.get("nickname"),
                            current_step.get("season"),
                            current_step.get("number"),
                            current_step.get("sponsored_team"),
                            rider_team.get("id"),
                            rider_category.get("id"),
                            rider_category.get("name"),
                            rider_category.get("legacy_id"),
                            current_step.get("in_grid"),
                            current_step.get("short_nickname"),
                            current_step.get("current"),
                            (pictures.get("profile") or {}).get("main"),
                            (pictures.get("bike") or {}).get("main"),
                            (pictures.get("helmet") or {}).get("main"),
                            pictures.get("number"),
                            pictures.get("portrait"),
                            current_step.get("type"),
                            (rider.get("country") or {}).get("iso"),
                            (rider.get("country") or {}).get("name"),
                            (rider.get("country") or {}).get("flag"),
                            rider.get("birth_city"),
                            rider.get("birth_date"),
                            rider.get("years_old"),
                            rider.get("published"),
                            rider.get("legacy_id"),
                            year,
                            md5_hash,
                        )

                        cursor.execute(
                            """
                            INSERT INTO TeamRiders VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            values,
                        )
                        total += 1

        cnx.commit()

    logging.info("TeamRiders elaborati anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
