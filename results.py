import hashlib
import json
import logging
import time as time_module

from runtime import get_db_connection, get_http_session, parse_year_arg, request_json, setup_logging


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


def main() -> int:
    setup_logging()
    target_year = parse_year_arg()
    session = get_http_session()

    processed_sessions = 0
    processed_results = 0
    processed_records = 0

    with get_db_connection() as cnx:
        with cnx.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM sessions
                WHERE year = %s
                  AND date >= CURDATE() - INTERVAL 30 DAY
                  AND date < CURDATE() + INTERVAL 1 DAY
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
                try:
                    data = request_json(session, url)
                except Exception as exc:
                    logging.error("Errore fetch risultati session %s: %s", session_id, exc)
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

                    cursor.execute(
                        RESULTS_QUERY,
                        (
                            item.get("id"),
                            item.get("position"),
                            rider_id,
                            rider.get("full_name"),
                            rider_country.get("iso"),
                            rider_country.get("name"),
                            rider_country.get("region_iso"),
                            rider.get("legacy_id"),
                            rider.get("number"),
                            rider.get("riders_api_uuid"),
                            team.get("id"),
                            team.get("name"),
                            team.get("legacy_id"),
                            season.get("id"),
                            season.get("year"),
                            season.get("current"),
                            constructor.get("id"),
                            constructor.get("name"),
                            constructor.get("legacy_id"),
                            item.get("average_speed"),
                            gap.get("first"),
                            gap.get("lap"),
                            item.get("total_laps"),
                            item.get("time"),
                            item.get("points"),
                            item.get("status"),
                            file_url,
                            files,
                            session_id,
                            event_id,
                            row["year"],
                            digest,
                            gap.get("prev"),
                            item.get("top_speed"),
                            best_lap.get("number"),
                            best_lap.get("time"),
                            row.get("track_condition"),
                            row.get("air_condition"),
                            row.get("humidity_condition"),
                            row.get("ground_condition"),
                            row.get("weather_condition"),
                            category_id,
                            row.get("number"),
                            row.get("circuit_name"),
                            row.get("type"),
                            row.get("category_name"),
                            row.get("event_name"),
                            row.get("event_sponsored_name"),
                            row.get("year"),
                            row.get("circuit_id"),
                            row.get("circuit_legacy_id"),
                            row.get("circuit_place"),
                            row.get("circuit_nation"),
                            row.get("country_iso"),
                            row.get("country_name"),
                            row.get("country_region_iso"),
                            row.get("event_short_name"),
                        ),
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

                    cursor.execute(
                        RECORDS_QUERY,
                        (
                            record_type,
                            rider_id,
                            rider.get("full_name"),
                            rider_country.get("iso"),
                            rider_country.get("name"),
                            rider_country.get("region_iso"),
                            rider.get("legacy_id"),
                            best_lap.get("number"),
                            best_lap.get("time"),
                            record.get("speed"),
                            record_year,
                            record.get("isNewRecord"),
                            event_id,
                            digest,
                            category_id,
                            row.get("category_name"),
                            row.get("event_name"),
                            row.get("event_sponsored_name"),
                            row.get("year"),
                            row.get("circuit_id"),
                            row.get("circuit_legacy_id"),
                            row.get("circuit_place"),
                            row.get("circuit_nation"),
                            row.get("country_iso"),
                            row.get("country_name"),
                            row.get("country_region_iso"),
                            row.get("event_short_name"),
                            session_id,
                        ),
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
