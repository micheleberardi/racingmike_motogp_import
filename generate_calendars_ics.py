import argparse
import hashlib
import html
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

from runtime import get_http_session, get_target_year, request_json, setup_logging

MOTOGP_RESULTS_BASE = "https://api.motogp.pulselive.com/motogp/v1/results"
MOTOGP_CALENDAR_URL = "https://www.motogp.com/en/calendar"
WORLDSBK_CALENDAR_URL = "https://www.worldsbk.com/en/calendar"
WORLDSBK_SITE_BASE = "https://www.worldsbk.com"

MOTOGP_CATEGORY_BY_LEGACY = {
    3: "MotoGP",
    2: "Moto2",
    1: "Moto3",
}

WORLDSBK_CATEGORIES = [
    "WorldSBK",
    "WorldSSP",
    "WorldSPB",
    "R3 bLU cRU Champ",
    "WorldWCR",
]

MOTOGP_TYPES_QUALIFY_AND_RACES = {"Q", "RAC", "SPR"}
MOTOGP_TYPES_RACES_ONLY = {"RAC", "SPR"}

WORLDSBK_EVENT_LINK_RE = re.compile(r'href=["\'](/en/event/[A-Z0-9]+/(?P<year>\d{4}))["\']')
WORLDSBK_TITLE_RE = re.compile(r'<h2 class="country-flag[^"]*">(?P<title>[^<]+)</h2>')
WORLDSBK_SESSION_RE = re.compile(
    r"<div class=\"timeIso\">.*?"
    r"<div data_ini=(?P<q1>['\"])(?P<start>.*?)(?P=q1)></div>.*?"
    r"<div data_end=(?P<q2>['\"])(?P<end>.*?)(?P=q2)></div>.*?"
    r"<div class=\"cat-session[^\"]*\"[^>]*>(?P<label>[^<]+)</div>",
    re.S,
)


@dataclass
class CalendarEvent:
    uid_seed: str
    start: datetime
    end: Optional[datetime]
    summary: str
    description: str
    location: str
    url: str
    category: str
    session_name: str


def parse_datetime(value: str) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None

    for candidate in (raw, raw.replace("Z", "+00:00")):
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
            try:
                parsed = datetime.strptime(candidate, fmt)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue

    if len(raw) > 6 and raw[-3] == ":" and raw[-6] in ("+", "-"):
        compact = f"{raw[:-3]}{raw[-2:]}"
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
            try:
                parsed = datetime.strptime(compact, fmt)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                continue

    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        logging.warning("Unable to parse datetime: %s", value)
        return None


def slugify(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    return normalized.strip("_")


def escape_ics_text(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace(",", "\\,")
        .replace(";", "\\;")
    )


def fold_ics_line(line: str, max_len: int = 75) -> List[str]:
    if len(line) <= max_len:
        return [line]
    chunks = [line[:max_len]]
    remaining = line[max_len:]
    while remaining:
        chunk_size = max_len - 1
        chunks.append(f" {remaining[:chunk_size]}")
        remaining = remaining[chunk_size:]
    return chunks


def format_utc_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def render_ics(calendar_name: str, events: Iterable[CalendarEvent]) -> str:
    dtstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ordered_events = sorted(events, key=lambda item: (item.start, item.summary, item.uid_seed))

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//racingmike//MotoGP and WorldSBK Calendars//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{escape_ics_text(calendar_name)}",
        "X-WR-TIMEZONE:UTC",
    ]

    for item in ordered_events:
        uid_hash = hashlib.md5(item.uid_seed.encode("utf-8")).hexdigest()
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid_hash}@racingmike.calendar",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART:{format_utc_timestamp(item.start)}",
            ]
        )
        if item.end:
            lines.append(f"DTEND:{format_utc_timestamp(item.end)}")
        lines.extend(
            [
                f"SUMMARY:{escape_ics_text(item.summary)}",
                f"DESCRIPTION:{escape_ics_text(item.description)}",
                f"LOCATION:{escape_ics_text(item.location)}",
                f"URL:{item.url}",
                "END:VEVENT",
            ]
        )

    lines.append("END:VCALENDAR")

    output_lines: List[str] = []
    for line in lines:
        output_lines.extend(fold_ics_line(line))
    return "\r\n".join(output_lines) + "\r\n"


def write_ics(path: Path, calendar_name: str, events: Iterable[CalendarEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = render_ics(calendar_name=calendar_name, events=events)
    path.write_text(content, encoding="utf-8")
    event_count = content.count("BEGIN:VEVENT")
    logging.info("Written %s (%s events)", path, event_count)


def fetch_motogp_season_uuid(session, year: int) -> str:
    seasons = request_json(session, f"{MOTOGP_RESULTS_BASE}/seasons")
    for season in seasons:
        if season.get("year") == year:
            season_uuid = season.get("id")
            if season_uuid:
                return season_uuid
    raise RuntimeError(f"MotoGP season {year} not found")


def fetch_motogp_categories(session, season_uuid: str) -> Dict[str, str]:
    categories = request_json(session, f"{MOTOGP_RESULTS_BASE}/categories?seasonUuid={season_uuid}")
    by_legacy = {category.get("legacy_id"): category for category in categories}

    category_ids: Dict[str, str] = {}
    for legacy_id, name in MOTOGP_CATEGORY_BY_LEGACY.items():
        category = by_legacy.get(legacy_id)
        if not category or not category.get("id"):
            raise RuntimeError(f"MotoGP category {name} not found for season")
        category_ids[name] = category["id"]
    return category_ids


def fetch_motogp_events(session, season_uuid: str, year: int) -> List[dict]:
    by_id: Dict[str, dict] = {}
    for is_finished in ("true", "false"):
        url = f"{MOTOGP_RESULTS_BASE}/events?seasonUuid={season_uuid}&isFinished={is_finished}"
        for event in request_json(session, url):
            event_id = event.get("id")
            if event_id:
                by_id[event_id] = event

    filtered = []
    for event in by_id.values():
        if event.get("test"):
            continue
        if not (event.get("date_start") or "").startswith(str(year)):
            continue
        filtered.append(event)

    filtered.sort(key=lambda item: item.get("date_start", ""))
    return filtered


def build_motogp_events_for_category(session, events: Iterable[dict], category_id: str, category_name: str) -> List[CalendarEvent]:
    output: List[CalendarEvent] = []
    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue

        sessions_url = f"{MOTOGP_RESULTS_BASE}/sessions?eventUuid={event_id}&categoryUuid={category_id}"
        for row in request_json(session, sessions_url):
            start = parse_datetime(row.get("date", ""))
            if not start:
                continue

            event_info = row.get("event") or {}
            circuit_info = event_info.get("circuit") or {}
            country_info = event_info.get("country") or {}
            session_type = (row.get("type") or "").strip() or "SESSION"

            event_name = (event_info.get("sponsored_name") or event_info.get("name") or event.get("name") or "").strip()
            short_name = (event_info.get("short_name") or event.get("short_name") or "").strip()
            location_parts = [circuit_info.get("name", "").strip(), country_info.get("name", "").strip()]
            location = ", ".join(part for part in location_parts if part) or "MotoGP Circuit"

            display_event = event_name or short_name or "MotoGP Grand Prix"
            summary = f"{category_name} - {session_type} - {display_event}"
            description_lines = [
                f"Championship: MotoGP",
                f"Category: {category_name}",
                f"Session: {session_type}",
                f"Event: {display_event}",
                f"Source: {MOTOGP_CALENDAR_URL}",
            ]
            description = "\n".join(description_lines)
            session_id = row.get("id") or f"{event_id}-{category_id}-{session_type}-{start.isoformat()}"

            output.append(
                CalendarEvent(
                    uid_seed=f"motogp|{session_id}|{category_name}",
                    start=start,
                    end=None,
                    summary=summary,
                    description=description,
                    location=location,
                    url=MOTOGP_CALENDAR_URL,
                    category=category_name,
                    session_name=session_type,
                )
            )
    return output


def fetch_worldsbk_event_urls(session, year: int) -> List[str]:
    response = session.get(WORLDSBK_CALENDAR_URL, timeout=20)
    response.raise_for_status()
    html_text = response.text

    seen = set()
    ordered: List[str] = []
    for match in WORLDSBK_EVENT_LINK_RE.finditer(html_text):
        if match.group("year") != str(year):
            continue
        relative = match.group(1)
        full_url = f"{WORLDSBK_SITE_BASE}{relative}"
        if full_url in seen:
            continue
        seen.add(full_url)
        ordered.append(full_url)
    return ordered


def parse_worldsbk_event_title(html_text: str, fallback: str) -> str:
    match = WORLDSBK_TITLE_RE.search(html_text)
    if not match:
        return fallback
    return " ".join(html.unescape(match.group("title")).split())


def build_worldsbk_events(session, year: int) -> Dict[str, List[CalendarEvent]]:
    event_urls = fetch_worldsbk_event_urls(session, year)
    if not event_urls:
        raise RuntimeError(f"No WorldSBK event URLs found for {year}")

    per_category: Dict[str, List[CalendarEvent]] = {name: [] for name in WORLDSBK_CATEGORIES}

    for event_url in event_urls:
        response = session.get(event_url, timeout=20)
        response.raise_for_status()
        html_text = response.text
        fallback_title = event_url.rstrip("/").split("/")[-2]
        event_title = parse_worldsbk_event_title(html_text, fallback=fallback_title)

        for match in WORLDSBK_SESSION_RE.finditer(html_text):
            start = parse_datetime(match.group("start"))
            if not start:
                continue

            end = parse_datetime(match.group("end"))
            label = " ".join(html.unescape(match.group("label")).split())
            if " - " not in label:
                continue

            category_name, session_name = [part.strip() for part in label.split(" - ", 1)]
            if category_name not in per_category:
                continue

            summary = f"{category_name} - {session_name} - {event_title}"
            description_lines = [
                "Championship: WorldSBK",
                f"Category: {category_name}",
                f"Session: {session_name}",
                f"Event: {event_title}",
                f"Source: {event_url}",
            ]
            description = "\n".join(description_lines)

            per_category[category_name].append(
                CalendarEvent(
                    uid_seed=f"worldsbk|{event_url}|{category_name}|{session_name}|{start.isoformat()}",
                    start=start,
                    end=end,
                    summary=summary,
                    description=description,
                    location=event_title,
                    url=event_url,
                    category=category_name,
                    session_name=session_name,
                )
            )

    for category_name in per_category:
        unique: Dict[str, CalendarEvent] = {}
        for item in per_category[category_name]:
            unique[item.uid_seed] = item
        per_category[category_name] = sorted(
            unique.values(),
            key=lambda event: (event.start, event.summary, event.uid_seed),
        )

    return per_category


def motogp_filter_all(_: CalendarEvent) -> bool:
    return True


def motogp_filter_qualify_and_races(event: CalendarEvent) -> bool:
    return event.session_name in MOTOGP_TYPES_QUALIFY_AND_RACES


def motogp_filter_races_only(event: CalendarEvent) -> bool:
    return event.session_name in MOTOGP_TYPES_RACES_ONLY


def worldsbk_filter_all(_: CalendarEvent) -> bool:
    return True


def worldsbk_filter_superpole_and_races(event: CalendarEvent) -> bool:
    lowered = event.session_name.lower()
    return ("superpole" in lowered) or ("race" in lowered)


def worldsbk_filter_races_only(event: CalendarEvent) -> bool:
    return "race" in event.session_name.lower()


def write_motogp_ics_files(base_dir: Path, year: int, events_by_category: Dict[str, List[CalendarEvent]]) -> None:
    filters: List[tuple[str, str, Callable[[CalendarEvent], bool]]] = [
        ("all_sessions", "All Sessions", motogp_filter_all),
        ("qualify_plus_races_only", "Qualify + Races Only", motogp_filter_qualify_and_races),
        ("races_only", "Races Only", motogp_filter_races_only),
    ]

    for category_name, source_events in events_by_category.items():
        category_slug = slugify(category_name)
        for filter_slug, filter_title, predicate in filters:
            selected_events = [item for item in source_events if predicate(item)]
            file_name = f"{category_slug}_{year}_{filter_slug}.ics"
            calendar_title = f"{category_name} {year} - {filter_title}"
            write_ics(base_dir / file_name, calendar_title, selected_events)


def write_worldsbk_ics_files(base_dir: Path, year: int, events_by_category: Dict[str, List[CalendarEvent]]) -> None:
    filters: List[tuple[str, str, Callable[[CalendarEvent], bool]]] = [
        ("all_sessions", "All Sessions", worldsbk_filter_all),
        ("superpole_plus_races_only", "Superpole + Races Only", worldsbk_filter_superpole_and_races),
        ("races_only", "Races Only", worldsbk_filter_races_only),
    ]

    for category_name, source_events in events_by_category.items():
        category_slug = slugify(category_name)
        for filter_slug, filter_title, predicate in filters:
            selected_events = [item for item in source_events if predicate(item)]
            file_name = f"{category_slug}_{year}_{filter_slug}.ics"
            calendar_title = f"{category_name} {year} - {filter_title}"
            write_ics(base_dir / file_name, calendar_title, selected_events)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate MotoGP and WorldSBK ICS calendars")
    parser.add_argument("--year", type=int, default=None, help="Season year (defaults to TARGET_YEAR or current UTC year)")
    parser.add_argument("--output-dir", default="calendars", help="Output base directory for generated ICS files")
    return parser.parse_args()


def main() -> int:
    setup_logging()
    args = parse_args()
    year = get_target_year(args.year)
    output_dir = Path(args.output_dir).resolve()
    logging.info("Generating ICS calendars for year %s", year)

    session = get_http_session()

    motogp_season_uuid = fetch_motogp_season_uuid(session, year)
    motogp_category_ids = fetch_motogp_categories(session, motogp_season_uuid)
    motogp_events = fetch_motogp_events(session, motogp_season_uuid, year)
    logging.info("MotoGP events found: %s", len(motogp_events))

    motogp_by_category: Dict[str, List[CalendarEvent]] = {}
    for category_name, category_id in motogp_category_ids.items():
        category_events = build_motogp_events_for_category(
            session=session,
            events=motogp_events,
            category_id=category_id,
            category_name=category_name,
        )
        motogp_by_category[category_name] = sorted(
            category_events,
            key=lambda event: (event.start, event.summary, event.uid_seed),
        )
        logging.info("MotoGP sessions collected for %s: %s", category_name, len(category_events))

    worldsbk_by_category = build_worldsbk_events(session, year)
    for category_name in WORLDSBK_CATEGORIES:
        logging.info(
            "WorldSBK sessions collected for %s: %s",
            category_name,
            len(worldsbk_by_category.get(category_name, [])),
        )

    write_motogp_ics_files(output_dir / str(year) / "motogp", year, motogp_by_category)
    write_worldsbk_ics_files(output_dir / str(year) / "worldsbk", year, worldsbk_by_category)

    logging.info("ICS calendars generated in %s", output_dir / str(year))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
