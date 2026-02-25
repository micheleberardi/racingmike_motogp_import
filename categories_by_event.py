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
                SELECT id, year
                FROM events
                WHERE year = %s
                  AND test != 1
                  AND date_start BETWEEN CURDATE() - INTERVAL 50 DAY AND CURDATE()
                ORDER BY date_start DESC
                """,
                (target_year,),
            )
            events = cursor.fetchall()

            for event in events:
                event_id = event["id"]
                year = event["year"]
                url = f"https://api.motogp.pulselive.com/motogp/v1/results/categories?eventUuid={event_id}"
                try:
                    categories = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore categorie evento %s: %s", event_id, exc)
                    continue

                for item in categories:
                    category_id = item.get("id")
                    legacy_id = item.get("legacy_id")
                    name = item.get("name")
                    md5 = hashlib.md5(f"{category_id}{legacy_id}{name}{year}".encode("utf-8")).hexdigest()
                    cursor.execute(
                        """
                        INSERT INTO categories_by_event (id, legacy_id, name, year, md5)
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

    logging.info("Categorie per evento elaborate anno %s: %s", target_year, total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
