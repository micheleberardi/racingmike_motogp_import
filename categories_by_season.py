import hashlib
import logging

from runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    total = 0
    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT year
                FROM events
                WHERE year = %s
                  AND test != 1
                  AND date_start BETWEEN CURDATE() - INTERVAL 5 DAY AND CURDATE()
                """,
                (target_year,),
            )
            years = cursor.fetchall()

            for row in years:
                year = row["year"]
                url = f"https://api.motogp.pulselive.com/motogp/v1/categories?seasonYear={year}"
                try:
                    categories = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore categorie by season %s: %s", year, exc)
                    continue

                for category in categories:
                    category_id = category.get("id")
                    legacy_id = category.get("legacy_id")
                    name = category.get("name")
                    md5 = hashlib.md5(f"{category_id}{legacy_id}{name}{year}".encode("utf-8")).hexdigest()
                    cursor.execute(
                        """
                        INSERT INTO categories_by_season (id, legacy_id, name, year, md5)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            legacy_id = VALUES(legacy_id),
                            name = VALUES(name),
                            year = VALUES(year),
                            md5 = VALUES(md5)
                        """,
                        (category_id, legacy_id, name, year, md5),
                    )
                    total += 1

        cnx.commit()

    logging.info("Categorie per season elaborate anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
