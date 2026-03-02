# MotoGP Data Import Toolkit

Python scripts to import MotoGP data from public endpoints into a MySQL database.

This project is designed for recurring seasonal imports and live updates, with robust HTTP handling, configurable target year, and idempotent upserts.

## Public Results Link

View results and dashboard here:

- `https://api.micheleberardi.com/racingmike/`

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
python3 calendar2024.py
```

### ICS calendar generator (MotoGP + WorldSBK)

Generate the 2026 calendars in `.ics` format:

```bash
python3 generate_calendars_ics.py --year 2026
```

Default output directory:

`calendars/2026/`

- `calendars/2026/motogp/` (9 files)
  - `motogp_2026_all_sessions.ics`
  - `moto2_2026_all_sessions.ics`
  - `moto3_2026_all_sessions.ics`
  - `motogp_2026_qualify_plus_races_only.ics`
  - `moto2_2026_qualify_plus_races_only.ics`
  - `moto3_2026_qualify_plus_races_only.ics`
  - `motogp_2026_races_only.ics`
  - `moto2_2026_races_only.ics`
  - `moto3_2026_races_only.ics`

- `calendars/2026/worldsbk/` (15 files)
  - `worldsbk_2026_all_sessions.ics`
  - `worldssp_2026_all_sessions.ics`
  - `worldspb_2026_all_sessions.ics`
  - `r3_blu_cru_champ_2026_all_sessions.ics`
  - `worldwcr_2026_all_sessions.ics`
  - `worldsbk_2026_superpole_plus_races_only.ics`
  - `worldssp_2026_superpole_plus_races_only.ics`
  - `worldspb_2026_superpole_plus_races_only.ics`
  - `r3_blu_cru_champ_2026_superpole_plus_races_only.ics`
  - `worldwcr_2026_superpole_plus_races_only.ics`
  - `worldsbk_2026_races_only.ics`
  - `worldssp_2026_races_only.ics`
  - `worldspb_2026_races_only.ics`
  - `r3_blu_cru_champ_2026_races_only.ics`
  - `worldwcr_2026_races_only.ics`

Optional custom output directory:

```bash
python3 generate_calendars_ics.py --year 2026 --output-dir /path/to/output
```

### Pipeline runner (events -> sessions -> results)

```bash
# one-shot run
python3 run_pipeline.py --year 2026

# continuous loop every 15 minutes
python3 run_pipeline.py --year 2026 --loop --sleep-seconds 900
```

### Streamlit dashboard (results, standings, stats)

Start local UI:

```bash
streamlit run streamlit_dashboard.py
```

What it shows:

- Season overview (finished/upcoming rounds)
- Championship standings by category
- Session results by event/session
- "Stats Lab" with custom metrics (`Power Index`, `Consistency`) and a what-if simulator

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

## Publish Download Page (GitHub Pages)

This repository includes a ready markdown page with clickable calendar files:

- `index.md`

To publish it:

1. Push the repository to GitHub.
2. In GitHub: **Settings -> Pages**:
   - **Source**: `Deploy from a branch`
   - **Branch**: `main` (or your default branch), folder `/ (root)`
3. Save and wait for deployment.

Your public page URL will be:

- `https://micheleberardi.github.io/racingmike_motogp_import/`

From that page, users can click each `.ics` and download/import it.
