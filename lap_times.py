"""
Imports per-lap times from the official MotoGP Analysis PDFs.

The PDF (analysis_url column in sessions) contains one rider per half-page in
a two-column layout with: Lap#, LapTime, T1, T2, T3, T4, Speed.
On some pages the right-column rider header is rendered as a graphic (not
text), so pdfplumber cannot read the name.  In that case the rider is resolved
by cross-referencing the best lap time against the results table.
"""
import hashlib
import io
import logging
import re
import time as time_module
from typing import Optional

import requests

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from runtime import (
    get_db_connection,
    get_http_session,
    parse_year_arg,
    setup_logging,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LAP_TIME_RE = re.compile(r"^(\d+)'(\d{2}\.\d+)$")   # e.g. 1'30.124
SECTOR_RE   = re.compile(r"^\d{1,2}\.\d{1,3}$")       # e.g. 20.212
SPEED_RE    = re.compile(r"^\d{3}(\.\d)?$")            # e.g. 328.9 or 241
LAP_NUM_RE  = re.compile(r"^\d{1,3}$")
POSITION_RE = re.compile(r"^(\d+)(st|nd|rd|th)$")

LEFT_MAX_X  = 295.0  # x < this → left column


# ---------------------------------------------------------------------------
# Time conversion helpers
# ---------------------------------------------------------------------------

def lap_time_to_ms(raw: str) -> Optional[int]:
    """'1'30.124' → 90124 ms.  Returns None if unparseable."""
    m = LAP_TIME_RE.match(raw)
    if m:
        return int(m.group(1)) * 60_000 + round(float(m.group(2)) * 1000)
    return None


def sector_to_ms(raw: str) -> Optional[int]:
    """'20.212' → 20212 ms."""
    try:
        return round(float(raw) * 1000)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def _parse_page(words: list) -> list:
    """
    Parse one PDF page.  Returns a list of rider dicts, each containing:
        rider_name, rider_number, position, laps (list of lap dicts).
    Rider header can be None for the right-column first rider when the header
    is stored as a graphic.
    """
    left  = [w for w in words if w["x0"] <  LEFT_MAX_X]
    right = [w for w in words if w["x0"] >= LEFT_MAX_X]
    riders = []
    for col_words in (left, right):
        riders.extend(_parse_column(col_words))
    return riders


def _parse_column(words: list) -> list:
    """
    Parse one column (left or right) and return a list of rider blocks.
    Each block: {rider_name, rider_number, position, laps:[...]}
    """
    if not words:
        return []

    # Sort top-to-bottom
    rows = _group_rows(words)
    riders = []
    current_rider: Optional[dict] = None
    in_header = False

    for row_words in rows:
        text = " ".join(w["text"] for w in row_words)

        # Detect "Runs=N" → start of a rider header block
        if "Runs=" in text:
            if current_rider is None:
                # No visible header preceded this → create anonymous placeholder
                current_rider = {"rider_name": None, "rider_number": None, "position": None, "laps": []}
            in_header = True
            continue

        # Detect "Run #" → tyre line, marks end of header, data follows
        if "Run #" in text:
            in_header = False
            continue

        # Detect rider name line: "Firstname LASTNAME CONSTRUCTOR NAT"
        # Pattern: multiple words, last two are short (constructor ≤ 10, nat = 3)
        name_match = _try_parse_name_line(row_words)
        if name_match:
            if current_rider and current_rider["laps"]:
                riders.append(current_rider)
            current_rider = {"laps": [], **name_match}
            in_header = True
            continue

        # Detect "Nth NN" (position + number) line
        pos_match = _try_parse_position_line(row_words)
        if pos_match and current_rider is not None and in_header:
            current_rider.update(pos_match)
            continue

        if in_header:
            continue

        # Try to parse as a lap data row
        lap = _try_parse_lap_row(row_words)
        if lap is not None:
            if current_rider is None:
                # Orphan lap data — create anonymous rider
                current_rider = {"rider_name": None, "rider_number": None, "position": None, "laps": []}
            current_rider["laps"].append(lap)

    if current_rider and current_rider["laps"]:
        riders.append(current_rider)

    return riders


def _group_rows(words: list, y_tol: float = 4.0) -> list:
    """Group words by y-coordinate into rows."""
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    rows: list = []
    current_row = [sorted_words[0]]
    for w in sorted_words[1:]:
        if abs(w["top"] - current_row[0]["top"]) <= y_tol:
            current_row.append(w)
        else:
            rows.append(sorted(current_row, key=lambda x: x["x0"]))
            current_row = [w]
    rows.append(sorted(current_row, key=lambda x: x["x0"]))
    return rows


def _try_parse_name_line(row_words: list) -> Optional[dict]:
    """
    Detect a rider-name line like 'Marco BEZZECCHI  APRILIA  ITA'
    Rules: ≥3 tokens, last token is a 2-3 letter country code (all caps),
    second-to-last is a constructor (all caps, ≤12 chars).
    """
    tokens = [w["text"] for w in row_words]
    if len(tokens) < 3:
        return None
    country = tokens[-1]
    constructor = tokens[-2]
    if not (re.match(r"^[A-Z]{2,3}$", country) and re.match(r"^[A-Z]{2,12}$", constructor)):
        return None
    name_parts = tokens[:-2]
    full_name = " ".join(name_parts)
    return {"rider_name": full_name, "rider_number": None, "position": None}


def _try_parse_position_line(row_words: list) -> Optional[dict]:
    """
    Detect 'Nth NN' (ordinal + race number) e.g. '1st 72'.
    """
    tokens = [w["text"] for w in row_words]
    if len(tokens) < 2:
        return None
    if POSITION_RE.match(tokens[0]) and tokens[1].isdigit():
        return {"position": int(POSITION_RE.match(tokens[0]).group(1)), "rider_number": int(tokens[1])}
    return None


def _try_parse_lap_row(row_words: list) -> Optional[dict]:
    """
    Detect a lap data row: lap_num  lap_time  t1  t2  t3  t4  speed
    May be missing t4 and speed (DNF/pit laps).
    """
    tokens = [w["text"] for w in row_words]
    if len(tokens) < 3:
        return None
    if not LAP_NUM_RE.match(tokens[0]):
        return None
    if not LAP_TIME_RE.match(tokens[1]):
        return None

    lap_num = int(tokens[0])
    lap_time = tokens[1]
    t1 = tokens[2] if len(tokens) > 2 and SECTOR_RE.match(tokens[2]) else None
    t2 = tokens[3] if len(tokens) > 3 and SECTOR_RE.match(tokens[3]) else None
    t3 = tokens[4] if len(tokens) > 4 and SECTOR_RE.match(tokens[4]) else None
    t4 = tokens[5] if len(tokens) > 5 and SECTOR_RE.match(tokens[5]) else None
    speed_raw = tokens[6] if len(tokens) > 6 else None
    speed = None
    if speed_raw:
        try:
            speed = float(speed_raw)
        except ValueError:
            pass

    return {
        "lap_number": lap_num,
        "lap_time":   lap_time,
        "t1": t1, "t2": t2, "t3": t3, "t4": t4,
        "speed": speed,
    }


def parse_analysis_pdf(pdf_bytes: bytes) -> list:
    """
    Parse a MotoGP Analysis PDF.
    Returns a flat list of rider dicts (each with .laps list).
    Rider blocks split across pages are merged by rider_number.
    """
    if pdfplumber is None:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")
    raw: list = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            raw.extend(_parse_page(words))

    # Merge blocks for the same rider split across pages
    merged: dict = {}   # rider_number → rider dict
    anonymous: list = []
    for block in raw:
        num = block.get("rider_number")
        if num is not None:
            if num in merged:
                existing = merged[num]
                existing["laps"].extend(block["laps"])
                # Prefer non-None name / position from later blocks
                if not existing.get("rider_name") and block.get("rider_name"):
                    existing["rider_name"] = block["rider_name"]
                if existing.get("position") is None and block.get("position") is not None:
                    existing["position"] = block["position"]
            else:
                merged[num] = {**block, "laps": list(block["laps"])}
        else:
            anonymous.append({**block, "laps": list(block["laps"])})

    # Sort laps within each rider by lap_number
    result = list(merged.values()) + anonymous
    for rider in result:
        rider["laps"].sort(key=lambda l: l["lap_number"])

    return result


# ---------------------------------------------------------------------------
# Rider identity resolution
# ---------------------------------------------------------------------------

def _resolve_riders(session_id: str, parsed_riders: list, cursor) -> list:
    """
    For riders whose name/number could not be parsed (graphical header),
    match them to known riders in the results table by best lap time.
    Returns parsed_riders with rider_id / rider_number / rider_name filled in
    where possible.
    """
    cursor.execute(
        """
        SELECT rider_id, rider_full_name, rider_number, best_lap_time, best_lap_number
        FROM results
        WHERE session_id = %s
          AND rider_id IS NOT NULL
        """,
        (session_id,),
    )
    db_results = {str(row["rider_id"]): row for row in cursor.fetchall()}
    if not db_results:
        return parsed_riders

    # Build a map: best_lap_time_string → rider info
    best_lap_map: dict = {}
    for row in db_results.values():
        blt = (row["best_lap_time"] or "").strip()
        if blt:
            best_lap_map[blt] = row

    # Set of rider_numbers already attributed
    attributed_numbers = {r.get("rider_number") for r in parsed_riders if r.get("rider_number")}

    for rider in parsed_riders:
        if rider.get("rider_id"):
            continue
        if rider.get("rider_number"):
            # Find by number
            for row in db_results.values():
                if row["rider_number"] == rider["rider_number"]:
                    rider.setdefault("rider_id", str(row["rider_id"]))
                    rider.setdefault("rider_name", rider.get("rider_name") or row["rider_full_name"])
                    break
            continue

        # No number available → try to match via best lap time
        for lap in rider["laps"]:
            if lap["lap_time"] in best_lap_map:
                match = best_lap_map[lap["lap_time"]]
                if match.get("best_lap_number") is None or match["best_lap_number"] == lap["lap_number"]:
                    if match["rider_number"] not in attributed_numbers:
                        rider["rider_id"]     = str(match["rider_id"])
                        rider["rider_number"] = match["rider_number"]
                        rider["rider_name"]   = rider.get("rider_name") or match["rider_full_name"]
                        attributed_numbers.add(match["rider_number"])
                        break

    # Final pass: fill rider_id from rider_number if still missing
    for rider in parsed_riders:
        if not rider.get("rider_id") and rider.get("rider_number"):
            for row in db_results.values():
                if row["rider_number"] == rider["rider_number"]:
                    rider["rider_id"] = str(row["rider_id"])
                    rider.setdefault("rider_name", row["rider_full_name"])
                    break

    return parsed_riders


# ---------------------------------------------------------------------------
# Database insertion
# ---------------------------------------------------------------------------

INSERT_QUERY = """
INSERT INTO lap_times (
    session_id, rider_id, rider_number, rider_name,
    lap_number, lap_time, lap_time_ms,
    t1, t2, t3, t4,
    t1_ms, t2_ms, t3_ms, t4_ms,
    speed, is_best_lap,
    session_type, year, event_id, category_id, category_name, event_name, circuit_name,
    md5
) VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s
)
ON DUPLICATE KEY UPDATE
    rider_id      = COALESCE(VALUES(rider_id), rider_id),
    rider_name    = COALESCE(VALUES(rider_name), rider_name),
    rider_number  = COALESCE(VALUES(rider_number), rider_number),
    lap_time      = VALUES(lap_time),
    lap_time_ms   = VALUES(lap_time_ms),
    t1 = VALUES(t1), t2 = VALUES(t2), t3 = VALUES(t3), t4 = VALUES(t4),
    t1_ms = VALUES(t1_ms), t2_ms = VALUES(t2_ms),
    t3_ms = VALUES(t3_ms), t4_ms = VALUES(t4_ms),
    speed         = VALUES(speed),
    is_best_lap   = VALUES(is_best_lap),
    md5           = VALUES(md5)
"""


def _insert_laps(cursor, session_row: dict, riders: list) -> int:
    """Insert all parsed laps for one session. Returns the number of rows inserted/updated."""
    inserted = 0
    session_id   = session_row["id"]
    session_type = session_row.get("type")
    year         = session_row.get("year")
    event_id     = session_row.get("event_id")
    category_id  = session_row.get("category_id")
    category_name = session_row.get("category_name")
    event_name   = session_row.get("event_name")
    circuit_name = session_row.get("circuit_name")

    for rider in riders:
        rider_id     = rider.get("rider_id")
        rider_number = rider.get("rider_number")
        rider_name   = rider.get("rider_name")

        # Find the best lap for this rider in this session
        best_lap_time_ms = None
        for lap in rider["laps"]:
            ms = lap_time_to_ms(lap["lap_time"] or "")
            if ms and (best_lap_time_ms is None or ms < best_lap_time_ms):
                best_lap_time_ms = ms

        for lap in rider["laps"]:
            lt_ms = lap_time_to_ms(lap["lap_time"] or "")
            is_best = 1 if lt_ms and lt_ms == best_lap_time_ms else 0

            digest = hashlib.md5(
                f"{session_id}|{rider_id}|{rider_number}|{lap['lap_number']}".encode()
            ).hexdigest()

            cursor.execute(
                INSERT_QUERY,
                (
                    session_id, rider_id, rider_number, rider_name,
                    lap["lap_number"], lap["lap_time"], lt_ms,
                    lap["t1"], lap["t2"], lap["t3"], lap["t4"],
                    sector_to_ms(lap["t1"]), sector_to_ms(lap["t2"]),
                    sector_to_ms(lap["t3"]), sector_to_ms(lap["t4"]),
                    lap["speed"], is_best,
                    session_type, year, event_id, category_id, category_name, event_name, circuit_name,
                    digest,
                ),
            )
            inserted += 1

    return inserted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    setup_logging()
    target_year = parse_year_arg()

    if pdfplumber is None:
        logging.error("pdfplumber is required. Install with: pip install pdfplumber")
        return 1

    http = get_http_session()
    total_sessions = 0
    total_laps = 0
    total_skipped = 0

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.id, s.type, s.year, s.event_id, s.category_id,
                       s.category_name, s.event_name, s.circuit_name,
                       s.analysis_url, s.status
                FROM sessions s
                WHERE s.year = %s
                  AND s.analysis_url IS NOT NULL
                  AND s.analysis_url != ''
                  AND COALESCE(s.status, '') = 'FINISHED'
                  AND s.type NOT IN ('WUP')
                ORDER BY s.session_datetime ASC
                """,
                (target_year,),
            )
            sessions = cursor.fetchall()

            for sess in sessions:
                session_id  = sess["id"]
                analysis_url = sess["analysis_url"]

                # Skip if we already have lap data for this session
                cursor.execute(
                    "SELECT COUNT(*) AS cnt FROM lap_times WHERE session_id = %s",
                    (session_id,),
                )
                existing = cursor.fetchone()["cnt"]
                if existing > 0:
                    logging.debug("Skip session %s (already %s laps)", session_id, existing)
                    total_skipped += 1
                    continue

                # Download PDF
                try:
                    resp = http.get(analysis_url, timeout=30)
                    resp.raise_for_status()
                except Exception as exc:
                    logging.warning("Cannot fetch PDF for session %s: %s", session_id, exc)
                    continue

                # Parse
                try:
                    riders = parse_analysis_pdf(resp.content)
                except Exception as exc:
                    logging.error("PDF parse error session %s: %s", session_id, exc)
                    continue

                if not riders:
                    logging.warning("No data parsed for session %s (%s)", session_id, analysis_url)
                    continue

                # Resolve anonymous riders
                riders = _resolve_riders(session_id, riders, cursor)

                # Insert
                n = _insert_laps(cursor, sess, riders)
                total_laps += n
                total_sessions += 1

                logging.info(
                    "Session %s | %s %s | riders=%s laps=%s",
                    session_id,
                    sess.get("event_name", ""),
                    sess.get("type", ""),
                    len(riders),
                    n,
                )
                time_module.sleep(0.3)

        cnx.commit()

    logging.info(
        "Lap times import anno %s | sessions=%s skipped=%s total_laps=%s",
        target_year,
        total_sessions,
        total_skipped,
        total_laps,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
