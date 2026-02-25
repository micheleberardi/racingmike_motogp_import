import json
import logging
from pathlib import Path

from modules.runtime import get_db_connection, parse_year_arg, setup_logging


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    output_path = Path(f"results_{target_year}.json")

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute("SELECT * FROM results WHERE year = %s", (target_year,))
            rows = cursor.fetchall()

    with output_path.open("w", encoding="utf-8") as file_handle:
        json.dump(rows, file_handle, indent=2, default=str)

    logging.info("Export completato: %s righe in %s", len(rows), output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
