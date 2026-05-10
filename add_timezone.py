"""
One-time migration: adds a `timezone` column to the events table and
backfills all rows using a static country_iso → IANA timezone mapping.

Run once on the server:
    python3.11 add_timezone.py

For US events the mapping also checks circuit_place (state) so that
historical venues (Laguna Seca, Indianapolis, Daytona) get the right zone.
"""

import logging
from runtime import get_db_connection, setup_logging

# Primary mapping: country_iso → IANA timezone
COUNTRY_TZ: dict[str, str] = {
    # Europe
    "AT": "Europe/Vienna",       # Austria - Red Bull Ring
    "DE": "Europe/Berlin",       # Germany - Sachsenring
    "ES": "Europe/Madrid",       # Spain - Jerez, Catalunya, Aragon, Valencia
    "FR": "Europe/Paris",        # France - Le Mans
    "GB": "Europe/London",       # UK - Silverstone, Donington
    "GR": "Europe/Athens",       # Greece
    "IT": "Europe/Rome",         # Italy - Mugello, Misano, Monza
    "NL": "Europe/Amsterdam",    # Netherlands - Assen
    "PT": "Europe/Lisbon",       # Portugal - Portimao, Estoril
    "FI": "Europe/Helsinki",     # Finland
    "CZ": "Europe/Prague",       # Czech Republic - Brno
    "HU": "Europe/Budapest",     # Hungary
    "SK": "Europe/Bratislava",   # Slovakia

    # Americas
    "AR": "America/Argentina/Buenos_Aires",  # Argentina - Termas de Rio Hondo
    "BR": "America/Sao_Paulo",   # Brazil - Interlagos
    "MX": "America/Mexico_City", # Mexico - Autodromo Hermanos Rodriguez
    "US": "America/Chicago",     # USA - COTA (Austin TX, Central Time)

    # Asia / Pacific
    "AU": "Australia/Melbourne", # Australia - Phillip Island
    "AZ": "Asia/Baku",           # Azerbaijan
    "CN": "Asia/Shanghai",       # China
    "ID": "Asia/Makassar",       # Indonesia - Lombok (WITA / UTC+8)
    "IN": "Asia/Kolkata",        # India
    "JP": "Asia/Tokyo",          # Japan - Motegi, Suzuka
    "KZ": "Asia/Almaty",         # Kazakhstan
    "MY": "Asia/Kuala_Lumpur",   # Malaysia - Sepang
    "QA": "Asia/Qatar",          # Qatar - Losail
    "SG": "Asia/Singapore",      # Singapore
    "TH": "Asia/Bangkok",        # Thailand - Chang
    "TR": "Europe/Istanbul",     # Turkey

    # Middle East / Africa
    "ZA": "Africa/Johannesburg", # South Africa - Kyalami
    "BH": "Asia/Bahrain",        # Bahrain

    # Default fallback
    "??": "UTC",
}

# Overrides keyed by (country_iso, circuit_place fragment) for multi-venue countries
CIRCUIT_TZ_OVERRIDES: list[tuple[str, str, str]] = [
    # US circuits by city/state
    ("US", "Austin",        "America/Chicago"),       # COTA
    ("US", "Texas",         "America/Chicago"),
    ("US", "Laguna",        "America/Los_Angeles"),   # Laguna Seca (historic)
    ("US", "Monterey",      "America/Los_Angeles"),
    ("US", "Indianapolis",  "America/Indiana/Indianapolis"),
    ("US", "Daytona",       "America/New_York"),
    # Australia circuits
    ("AU", "Phillip",       "Australia/Melbourne"),   # AEDT/AEST
    ("AU", "Darwin",        "Australia/Darwin"),      # ACST (no DST)
    # Spain: Canary Islands would be UTC+1 but no GP there
]


def get_timezone(country_iso: str, circuit_place: str | None) -> str:
    iso = (country_iso or "").strip().upper()
    place = (circuit_place or "").strip()

    # Check circuit-level overrides first
    for c_iso, place_fragment, tz in CIRCUIT_TZ_OVERRIDES:
        if iso == c_iso and place_fragment.lower() in place.lower():
            return tz

    return COUNTRY_TZ.get(iso, "UTC")


def main() -> int:
    setup_logging()

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            # Add column if it doesn't exist
            cursor.execute("""
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = 'events'
                  AND COLUMN_NAME  = 'timezone'
            """)
            row = cursor.fetchone()
            if row["cnt"] == 0:
                logging.info("Adding timezone column to events table…")
                cursor.execute(
                    "ALTER TABLE events ADD COLUMN timezone VARCHAR(64) DEFAULT 'UTC' AFTER country_name"
                )
                logging.info("Column added.")
            else:
                logging.info("timezone column already exists, backfilling…")

            # Fetch all events
            cursor.execute("SELECT id, country_iso, circuit_place FROM events")
            events = cursor.fetchall()

            updated = 0
            for ev in events:
                tz = get_timezone(ev["country_iso"], ev.get("circuit_place"))
                cursor.execute(
                    "UPDATE events SET timezone = %s WHERE id = %s",
                    (tz, ev["id"]),
                )
                updated += 1

        cnx.commit()

    logging.info("Timezone backfill complete — %d events updated.", updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
