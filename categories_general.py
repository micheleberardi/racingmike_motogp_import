import hashlib
import logging

from runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute("SELECT id, year FROM seasons WHERE year = %s", (target_year,))
            seasons = cursor.fetchall()

            total = 0
            for season in seasons:
                season_id = season["id"]
                year = season["year"]
                url = f"https://api.motogp.pulselive.com/motogp/v1/results/categories?seasonUuid={season_id}"
                try:
                    categories = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore categorie season %s: %s", season_id, exc)
                    continue

                for category in categories:
                    category_id = category.get("id")
                    legacy_id = category.get("legacy_id")
                    name = category.get("name")
                    md5 = hashlib.md5(f"{category_id}{legacy_id}{name}{year}".encode("utf-8")).hexdigest()

                    cursor.execute(
                        """
                        INSERT INTO categories_general (id, legacy_id, name, year, md5)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            legacy_id = VALUES(legacy_id),
                            name = VALUES(name),
                            md5 = VALUES(md5)
                        """,
                        (category_id, legacy_id, name, year, md5),
                    )
                    total += 1

        cnx.commit()

    logging.info("Categorie generali elaborate anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
