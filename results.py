import hashlib
import json
import logging
import time as time_module

from requests import HTTPError

from runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging

RESULTS_FIELDS = [
    "result_id",
    "position",
    "rider_id",
    "rider_full_name",
    "rider_country_iso",
    "rider_country_name",
    "rider_region_iso",
    "rider_legacy_id",
    "rider_number",
    "riders_api_uuid",
    "team_id",
    "team_name",
    "team_legacy_id",
    "team_season_id",
    "team_season_year",
    "team_season_current",
    "constructor_id",
    "constructor_name",
    "constructor_legacy_id",
    "average_speed",
    "gap_first",
    "gap_lap",
    "total_laps",
    "time",
    "points",
    "status",
    "file",
    "files",
    "session_id",
    "event_id",
    "year",
    "md5",
    "gap_prev",
    "top_speed",
    "best_lap_number",
    "best_lap_time",
    "track_condition",
    "air_condition",
    "humidity_condition",
    "ground_condition",
    "weather_condition",
    "category_id",
    "session_number",
    "circuit_name",
    "session_type",
    "category_name",
    "event_name",
    "event_sponsored_name",
    "event_season",
    "circuit_id",
    "circuit_legacy_id",
    "circuit_place",
    "circuit_nation",
    "circuit_country_iso",
    "circuit_country_name",
    "circuit_country_region_iso",
    "event_short_name",
]

RECORDS_FIELDS = [
    "record_type",
    "rider_id",
    "rider_full_name",
    "rider_country_iso",
    "rider_country_name",
    "rider_region_iso",
    "rider_legacy_id",
    "bestLap_number",
    "bestLap_time",
    "speed",
    "record_year",
    "isNewRecord",
    "event_id",
    "md5",
    "category_id",
    "category_name",
    "event_name",
    "event_sponsored_name",
    "year",
    "circuit_id",
    "circuit_legacy_id",
    "circuit_place",
    "circuit_nation",
    "circuit_country_iso",
    "circuit_country_name",
    "circuit_country_region_iso",
    "event_short_name",
    "session_id",
]


RESULTS_QUERY = """
INSERT INTO results (
    result_id, position, rider_id, rider_full_name, rider_country_iso,
    rider_country_name, rider_region_iso, rider_legacy_id, rider_number,
    riders_api_uuid, team_id, team_name, team_legacy_id, team_season_id,
    team_season_year, team_season_current, constructor_id, constructor_name,
    constructor_legacy_id, average_speed, gap_first, gap_lap, total_laps,
    time, points, status, file, files, session_id, event_id, year, md5,
    gap_prev, top_speed, best_lap_number, best_lap_time, track_condition,
    air_condition, humidity_condition, ground_condition, weather_condition,
    category_id, session_number, circuit_name, session_type, category_name,
    event_name, event_sponsored_name, event_season, circuit_id, circuit_legacy_id,
    circuit_place, circuit_nation, circuit_country_iso, circuit_country_name,
    circuit_country_region_iso, event_short_name
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    position = VALUES(position),
    rider_full_name = VALUES(rider_full_name),
    rider_country_iso = VALUES(rider_country_iso),
    rider_country_name = VALUES(rider_country_name),
    rider_region_iso = VALUES(rider_region_iso),
    rider_legacy_id = VALUES(rider_legacy_id),
    rider_number = VALUES(rider_number),
    riders_api_uuid = VALUES(riders_api_uuid),
    team_id = VALUES(team_id),
    team_name = VALUES(team_name),
    team_legacy_id = VALUES(team_legacy_id),
    team_season_id = VALUES(team_season_id),
    team_season_year = VALUES(team_season_year),
    team_season_current = VALUES(team_season_current),
    constructor_id = VALUES(constructor_id),
    constructor_name = VALUES(constructor_name),
    constructor_legacy_id = VALUES(constructor_legacy_id),
    average_speed = VALUES(average_speed),
    gap_first = VALUES(gap_first),
    gap_lap = VALUES(gap_lap),
    total_laps = VALUES(total_laps),
    time = VALUES(time),
    points = VALUES(points),
    status = VALUES(status),
    file = VALUES(file),
    files = VALUES(files),
    md5 = VALUES(md5),
    gap_prev = VALUES(gap_prev),
    top_speed = VALUES(top_speed),
    best_lap_number = VALUES(best_lap_number),
    best_lap_time = VALUES(best_lap_time),
    track_condition = VALUES(track_condition),
    air_condition = VALUES(air_condition),
    humidity_condition = VALUES(humidity_condition),
    ground_condition = VALUES(ground_condition),
    weather_condition = VALUES(weather_condition),
    category_id = VALUES(category_id),
    session_number = VALUES(session_number),
    circuit_name = VALUES(circuit_name),
    session_type = VALUES(session_type),
    category_name = VALUES(category_name),
    event_name = VALUES(event_name),
    event_sponsored_name = VALUES(event_sponsored_name),
    event_season = VALUES(event_season),
    circuit_id = VALUES(circuit_id),
    circuit_legacy_id = VALUES(circuit_legacy_id),
    circuit_place = VALUES(circuit_place),
    circuit_nation = VALUES(circuit_nation),
    circuit_country_iso = VALUES(circuit_country_iso),
    circuit_country_name = VALUES(circuit_country_name),
    circuit_country_region_iso = VALUES(circuit_country_region_iso),
    event_short_name = VALUES(event_short_name)
"""

RECORDS_QUERY = """
INSERT INTO records (
    record_type, rider_id, rider_full_name, rider_country_iso,
    rider_country_name, rider_region_iso, rider_legacy_id, bestLap_number,
    bestLap_time, speed, record_year, isNewRecord, event_id, md5, category_id,
    category_name, event_name, event_sponsored_name, year, circuit_id,
    circuit_legacy_id, circuit_place, circuit_nation, circuit_country_iso,
    circuit_country_name, circuit_country_region_iso, event_short_name, session_id
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    rider_full_name = VALUES(rider_full_name),
    rider_country_iso = VALUES(rider_country_iso),
    rider_country_name = VALUES(rider_country_name),
    rider_region_iso = VALUES(rider_region_iso),
    rider_legacy_id = VALUES(rider_legacy_id),
    bestLap_number = VALUES(bestLap_number),
    bestLap_time = VALUES(bestLap_time),
    speed = VALUES(speed),
    record_year = VALUES(record_year),
    isNewRecord = VALUES(isNewRecord),
    md5 = VALUES(md5),
    category_name = VALUES(category_name),
    event_name = VALUES(event_name),
    event_sponsored_name = VALUES(event_sponsored_name),
    year = VALUES(year),
    circuit_id = VALUES(circuit_id),
    circuit_legacy_id = VALUES(circuit_legacy_id),
    circuit_place = VALUES(circuit_place),
    circuit_nation = VALUES(circuit_nation),
    circuit_country_iso = VALUES(circuit_country_iso),
    circuit_country_name = VALUES(circuit_country_name),
    circuit_country_region_iso = VALUES(circuit_country_region_iso),
    event_short_name = VALUES(event_short_name)
"""


def fetch_classification_with_retry(session, url: str, session_id: str, max_attempts: int = 4):
    for attempt in range(1, max_attempts + 1):
        try:
            return request_json(session, url)
        except HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            should_retry = status_code in (403, 429, 500, 502, 503, 504)
            if should_retry and attempt < max_attempts:
                wait_seconds = 1.5 * attempt
                logging.warning(
                    "Retry %s/%s session %s after HTTP %s (sleep %.1fs)",
                    attempt,
                    max_attempts,
                    session_id,
                    status_code,
                    wait_seconds,
                )
                time_module.sleep(wait_seconds)
                continue
            logging.error("Errore fetch risultati session %s: %s", session_id, exc)
            return None
        except Exception as exc:
            if attempt < max_attempts:
                wait_seconds = 1.0 * attempt
                logging.warning(
                    "Retry %s/%s session %s after generic error (%s) (sleep %.1fs)",
                    attempt,
                    max_attempts,
                    session_id,
                    exc,
                    wait_seconds,
                )
                time_module.sleep(wait_seconds)
                continue
            logging.error("Errore fetch risultati session %s: %s", session_id, exc)
            return None

    return None


def get_text_column_limits(cursor, table_name: str) -> dict[str, int]:
    cursor.execute(
        """
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND data_type IN ('char', 'varchar', 'text', 'mediumtext', 'longtext')
        """,
        (table_name,),
    )
    return {
        row["column_name"]: int(row["character_maximum_length"])
        for row in cursor.fetchall()
        if row.get("character_maximum_length")
    }


def normalize_text_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, str):
        return value
    return str(value)


def sanitize_payload(payload: dict, text_limits: dict[str, int], context: str) -> dict:
    sanitized = dict(payload)
    for field_name, max_len in text_limits.items():
        if field_name not in sanitized:
            continue
        value = normalize_text_value(sanitized[field_name])
        if value is None:
            sanitized[field_name] = None
            continue
        if len(value) > max_len:
            logging.warning(
                "Truncating %s for %s from %s to %s chars",
                field_name,
                context,
                len(value),
                max_len,
            )
            value = value[:max_len]
        sanitized[field_name] = value
    return sanitized


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    processed_sessions = 0
    processed_results = 0
    processed_records = 0

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            results_text_limits = get_text_column_limits(cursor, "results")
            records_text_limits = get_text_column_limits(cursor, "records")

            cursor.execute(
                """
                SELECT *
                FROM sessions
                WHERE year = %s
                  AND COALESCE(status, '') = 'FINISHED'
                ORDER BY date DESC
                """,
                (target_year,),
            )
            sessions = cursor.fetchall()

            for row in sessions:
                session_id = row["id"]
                event_id = row["event_id"]
                category_id = row["category_id"]

                url = (
                    "https://api.motogp.pulselive.com/motogp/v1/results/session/"
                    f"{session_id}/classification?test=false"
                )
                data = fetch_classification_with_retry(session, url, session_id)
                if data is None:
                    continue

                classifications = data.get("classification", [])
                file_url = data.get("file", "")
                files = data.get("files")
                records = data.get("records") or []

                for item in classifications:
                    rider = item.get("rider") or {}
                    rider_id = rider.get("id")
                    if not rider_id:
                        continue

                    rider_country = rider.get("country") or {}
                    team = item.get("team") or {}
                    season = team.get("season") or {}
                    constructor = item.get("constructor") or {}
                    gap = item.get("gap") or {}
                    best_lap = item.get("best_lap") or {}

                    digest = hashlib.md5(
                        f"{rider_id}{session_id}{event_id}{row['year']}{category_id}".encode("utf-8")
                    ).hexdigest()

                    result_payload = sanitize_payload(
                        {
                            "result_id": item.get("id"),
                            "position": item.get("position"),
                            "rider_id": rider_id,
                            "rider_full_name": rider.get("full_name"),
                            "rider_country_iso": rider_country.get("iso"),
                            "rider_country_name": rider_country.get("name"),
                            "rider_region_iso": rider_country.get("region_iso"),
                            "rider_legacy_id": rider.get("legacy_id"),
                            "rider_number": rider.get("number"),
                            "riders_api_uuid": rider.get("riders_api_uuid"),
                            "team_id": team.get("id"),
                            "team_name": team.get("name"),
                            "team_legacy_id": team.get("legacy_id"),
                            "team_season_id": season.get("id"),
                            "team_season_year": season.get("year"),
                            "team_season_current": season.get("current"),
                            "constructor_id": constructor.get("id"),
                            "constructor_name": constructor.get("name"),
                            "constructor_legacy_id": constructor.get("legacy_id"),
                            "average_speed": item.get("average_speed"),
                            "gap_first": gap.get("first"),
                            "gap_lap": gap.get("lap"),
                            "total_laps": item.get("total_laps"),
                            "time": item.get("time"),
                            "points": item.get("points"),
                            "status": item.get("status"),
                            "file": file_url,
                            "files": files,
                            "session_id": session_id,
                            "event_id": event_id,
                            "year": row["year"],
                            "md5": digest,
                            "gap_prev": gap.get("prev"),
                            "top_speed": item.get("top_speed"),
                            "best_lap_number": best_lap.get("number"),
                            "best_lap_time": best_lap.get("time"),
                            "track_condition": row.get("track_condition"),
                            "air_condition": row.get("air_condition"),
                            "humidity_condition": row.get("humidity_condition"),
                            "ground_condition": row.get("ground_condition"),
                            "weather_condition": row.get("weather_condition"),
                            "category_id": category_id,
                            "session_number": row.get("number"),
                            "circuit_name": row.get("circuit_name"),
                            "session_type": row.get("type"),
                            "category_name": row.get("category_name"),
                            "event_name": row.get("event_name"),
                            "event_sponsored_name": row.get("event_sponsored_name"),
                            "event_season": row.get("year"),
                            "circuit_id": row.get("circuit_id"),
                            "circuit_legacy_id": row.get("circuit_legacy_id"),
                            "circuit_place": row.get("circuit_place"),
                            "circuit_nation": row.get("circuit_nation"),
                            "circuit_country_iso": row.get("country_iso"),
                            "circuit_country_name": row.get("country_name"),
                            "circuit_country_region_iso": row.get("country_region_iso"),
                            "event_short_name": row.get("event_short_name"),
                        },
                        results_text_limits,
                        f"results session={session_id} rider={rider_id}",
                    )

                    cursor.execute(
                        RESULTS_QUERY,
                        tuple(result_payload[field_name] for field_name in RESULTS_FIELDS),
                    )
                    processed_results += 1

                for record in records:
                    rider = record.get("rider") or {}
                    rider_country = rider.get("country") or {}
                    best_lap = record.get("bestLap") or {}
                    rider_id = rider.get("id")
                    if not rider_id:
                        continue

                    record_type = record.get("type")
                    record_year = record.get("year")
                    digest = hashlib.md5(
                        f"{record_type}{rider_id}{session_id}{event_id}{record_year}{category_id}".encode("utf-8")
                    ).hexdigest()

                    record_payload = sanitize_payload(
                        {
                            "record_type": record_type,
                            "rider_id": rider_id,
                            "rider_full_name": rider.get("full_name"),
                            "rider_country_iso": rider_country.get("iso"),
                            "rider_country_name": rider_country.get("name"),
                            "rider_region_iso": rider_country.get("region_iso"),
                            "rider_legacy_id": rider.get("legacy_id"),
                            "bestLap_number": best_lap.get("number"),
                            "bestLap_time": best_lap.get("time"),
                            "speed": record.get("speed"),
                            "record_year": record_year,
                            "isNewRecord": record.get("isNewRecord"),
                            "event_id": event_id,
                            "md5": digest,
                            "category_id": category_id,
                            "category_name": row.get("category_name"),
                            "event_name": row.get("event_name"),
                            "event_sponsored_name": row.get("event_sponsored_name"),
                            "year": row.get("year"),
                            "circuit_id": row.get("circuit_id"),
                            "circuit_legacy_id": row.get("circuit_legacy_id"),
                            "circuit_place": row.get("circuit_place"),
                            "circuit_nation": row.get("circuit_nation"),
                            "circuit_country_iso": row.get("country_iso"),
                            "circuit_country_name": row.get("country_name"),
                            "circuit_country_region_iso": row.get("country_region_iso"),
                            "event_short_name": row.get("event_short_name"),
                            "session_id": session_id,
                        },
                        records_text_limits,
                        f"records session={session_id} rider={rider_id} type={record_type}",
                    )

                    cursor.execute(
                        RECORDS_QUERY,
                        tuple(record_payload[field_name] for field_name in RECORDS_FIELDS),
                    )
                    processed_records += 1

                processed_sessions += 1
                time_module.sleep(0.2)

        cnx.commit()

    logging.info(
        "Risultati elaborati anno %s | sessions=%s results=%s records=%s",
        target_year,
        processed_sessions,
        processed_results,
        processed_records,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
