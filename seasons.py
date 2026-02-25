import hashlib
import logging

from modules.runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging

SEASONS_URL = "https://api.motogp.pulselive.com/motogp/v1/results/seasons"


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()

    session = get_http_session()
    try:
        seasons = request_json(session, SEASONS_URL)
    except Exception as exc:
        logging.error("Errore nel recupero stagioni: %s", exc)
        return 1

    inserted = 0
    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            for item in seasons:
                year = item.get("year")
                if year != target_year:
                    continue
                season_id = item.get("id")
                current = item.get("current")
                digest = hashlib.md5(f"{season_id}{year}{current}".encode("utf-8")).hexdigest()
                cursor.execute(
                    """
                    INSERT INTO seasons (id, name, year, current, md5)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        current = VALUES(current),
                        md5 = VALUES(md5)
                    """,
                    (season_id, item.get("name"), year, current, digest),
                )
                inserted += 1
        cnx.commit()

    logging.info("Stagioni elaborate per anno %s: %s", target_year, inserted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
