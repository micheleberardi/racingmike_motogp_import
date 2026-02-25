# MotoGP Data Import Toolkit

Python scripts to import MotoGP data from public endpoints into a MySQL database.

This project is designed for recurring seasonal imports and live updates, with robust HTTP handling, configurable target year, and idempotent upserts.

## Features

- Imports seasons, events, categories, sessions, results, standings, teams, and live timing.
- Supports yearly runs without code changes (`--year` or `TARGET_YEAR`).
- Centralized runtime utilities for:
  - environment loading
  - database connections
  - HTTP retry/timeout behavior
  - consistent logging
- Upsert-based writes for safe re-runs.

## Data Source

Data is fetched from MotoGP public API endpoints and related resources.

Primary endpoints used:

- `https://api.motogp.pulselive.com/motogp/v1/results/seasons`
- `https://api.motogp.pulselive.com/motogp/v1/results/events?...`
- `https://api.motogp.pulselive.com/motogp/v1/results/categories?...`
- `https://api.motogp.pulselive.com/motogp/v1/results/sessions?...`
- `https://api.motogp.pulselive.com/motogp/v1/results/session/{id}/classification?test=false`
- `https://api.motogp.pulselive.com/motogp/v1/results/standings?...`
- `https://api.motogp.pulselive.com/motogp/v1/teams?...`
- `https://api.motogp.pulselive.com/motogp/v1/timing-gateway/livetiming-lite`
- `https://mototiming.live/api/schedule?filter=all`

## Requirements

- Python 3.10+
- MySQL 8+
- Python packages:
  - `pymysql`
  - `requests`
  - `python-dotenv`
  - `urllib3`

Install dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in project root:

```env
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=your_user
DB_PASSWD=your_password
# optional alternative key supported:
# DB_PASSWORD=your_password
DB_NAME=your_database
DB_CHARSET=utf8mb4

# optional default year; if not set, current UTC year is used
TARGET_YEAR=2026
```

## Running Scripts

### Year-aware scripts

These scripts accept `--year`:

```bash
python3 seasons.py --year 2026
python3 events.py --year 2026
python3 categories_general.py --year 2026
python3 categories_by_event.py --year 2026
python3 categories_by_season.py --year 2026
python3 sessions.py --year 2026
python3 results.py --year 2026
python3 standingriders.py --year 2026
python3 teams.py --year 2026
python3 sessions_test.py --year 2026
python3 export_data.py --year 2026
```

### Non-year scripts

```bash
python3 liveresult.py
python3 calendar.py
```

## Suggested Execution Order

For a full seasonal refresh:

1. `seasons.py`
2. `events.py`
3. `categories_general.py`
4. `categories_by_event.py`
5. `categories_by_season.py`
6. `sessions.py`
7. `results.py`
8. `standingriders.py`
9. `teams.py`

For race weekend updates:

1. `sessions.py`
2. `results.py`
3. `standingriders.py`
4. `liveresult.py`

## Project Structure

- `runtime.py`: shared runtime helpers (DB, HTTP, year args, logging)
- `modules/runtime.py`: backward-compatible wrapper
- `modules/utils.py`: legacy utilities (JSON file helper, DB/API wrappers)
- `docs/DB_HARDENING.sql`: optional SQL improvements for keys/indexes
- `*.py`: import jobs for each data domain

## Notes About Data Behavior

- Some API payloads include duplicate legacy entries; deduplication is handled in event legacy import.
- `records` payload may contain multiple record types for the same rider/session; record hashing includes `record_type` to avoid collisions.
- Team endpoint can include riders from other categories/seasons; importer filters by requested category and year.
- Upcoming sessions are included by date window to support `NOT-STARTED` race weekends.

## Troubleshooting

### No rows imported

- Verify `.env` values and DB permissions.
- Confirm target year exists in seasons/events data.
- Check logs for HTTP errors and API status issues.

### Duplicate data concerns

- Ensure unique constraints in your DB match intended behavior.
- Re-run scripts safely: upserts are used across core tables.

### Encoding issues

- Use `utf8mb4` in MySQL and in `.env` (`DB_CHARSET=utf8mb4`).

## Disclaimer

This project is independent and not affiliated with MotoGP.
All trademarks and data rights belong to their respective owners.
Use the data responsibly and according to applicable terms.

## License

MIT (see `LICENSE.md`).
