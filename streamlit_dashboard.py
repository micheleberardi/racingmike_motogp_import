import logging
import math
import re
from hashlib import md5
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from runtime import get_db_connection, get_http_session, request_json

try:
    import altair as alt
except Exception:  # pragma: no cover - optional dependency fallback
    alt = None

RACE_SESSION_TYPES = ("RAC", "SPR", "Race 1", "Race 2", "Superpole Race")
FALLBACK_COLORS = (
    "#e10600",
    "#1e88e5",
    "#43a047",
    "#fb8c00",
    "#8e24aa",
    "#00897b",
    "#6d4c41",
    "#3949ab",
    "#f4511e",
    "#546e7a",
)
HEX_COLOR_RE = re.compile(r"^#(?:[0-9a-fA-F]{3}){1,2}$")
TEAM_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")

MOTOGP_RED = "#E8002D"
GOLD = "#FFD700"
SILVER = "#C0C0C0"
BRONZE = "#CD7F32"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw[:10]).date()
    except ValueError:
        return None


def _session_label(session: Dict[str, Any]) -> str:
    session_type = str(session.get("session_type") or "UNKNOWN").strip().upper()
    session_number = _safe_int(session.get("session_number"), default=0)

    if session_type in {"FP", "Q"} and session_number > 0:
        return f"{session_type}{session_number}"
    if session_type == "RAC":
        return "RACE"
    return session_type


def _normalize_hex_color(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if not raw.startswith("#"):
        raw = f"#{raw}"
    if HEX_COLOR_RE.match(raw):
        return raw.lower()
    return ""


def _fallback_color(key: str) -> str:
    digest = md5(key.encode("utf-8")).hexdigest()
    index = int(digest[:8], 16) % len(FALLBACK_COLORS)
    return FALLBACK_COLORS[index]


def _normalize_team_name(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return TEAM_NORMALIZE_RE.sub("", raw)


def _normalize_category_name(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = raw.replace("™", "").replace(" ", "")
    return TEAM_NORMALIZE_RE.sub("", raw)


def _hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    normalized = _normalize_hex_color(hex_color)
    if not normalized:
        return (0, 0, 0)
    value = normalized.lstrip("#")
    if len(value) == 3:
        value = "".join(ch * 2 for ch in value)
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4))


def _contrast_text_color(hex_color: str) -> str:
    r, g, b = _hex_to_rgb(hex_color)
    yiq = (r * 299 + g * 587 + b * 114) / 1000
    return "#111111" if yiq >= 150 else "#f5f5f5"


def _lighten_hex(hex_color: str, factor: float) -> str:
    normalized = _normalize_hex_color(hex_color)
    if not normalized:
        return ""
    ratio = min(max(factor, 0.0), 1.0)
    r, g, b = _hex_to_rgb(normalized)
    r = int(r + (255 - r) * ratio)
    g = int(g + (255 - g) * ratio)
    b = int(b + (255 - b) * ratio)
    return f"#{r:02x}{g:02x}{b:02x}"


def _display_team_color(hex_color: str) -> str:
    normalized = _normalize_hex_color(hex_color)
    if not normalized:
        return ""
    r, g, b = _hex_to_rgb(normalized)
    brightness = (r * 299 + g * 587 + b * 114) / 1000
    if brightness < 70:
        return _lighten_hex(normalized, 0.68)
    if brightness < 100:
        return _lighten_hex(normalized, 0.56)
    if brightness < 140:
        return _lighten_hex(normalized, 0.40)
    return _lighten_hex(normalized, 0.20)


def _team_color(team_name: Any, color_map: Dict[str, str]) -> str:
    team_key = _normalize_team_name(team_name)
    if team_key and team_key in color_map:
        return _display_team_color(color_map[team_key])
    fallback_key = team_key or str(team_name or "unknown")
    return _display_team_color(_fallback_color(fallback_key))


def _event_title(event: Dict[str, Any]) -> str:
    return (event.get("sponsored_name") or event.get("name") or "Unknown Event").strip()


def _db_fetchall(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    try:
        with get_db_connection() as cnx:
            with cnx.cursor() as cursor:
                cursor.execute(query, params)
                return list(cursor.fetchall())
    except Exception as exc:
        logging.warning("DB query failed: %s", exc)
        return []


@st.cache_data(ttl=1800, show_spinner=False)
def get_available_years() -> List[int]:
    rows = _db_fetchall(
        """
        SELECT DISTINCT year
        FROM seasons
        WHERE year IS NOT NULL
        ORDER BY year DESC
        """
    )
    years = [_safe_int(row.get("year")) for row in rows if _safe_int(row.get("year")) > 0]
    if years:
        return years

    fallback = _db_fetchall(
        """
        SELECT DISTINCT year
        FROM events
        WHERE year IS NOT NULL
        ORDER BY year DESC
        """
    )
    return [_safe_int(row.get("year")) for row in fallback if _safe_int(row.get("year")) > 0]


@st.cache_data(ttl=1800, show_spinner=False)
def get_categories_for_year(year: int) -> List[Dict[str, Any]]:
    rows = _db_fetchall(
        """
        SELECT DISTINCT id, name, COALESCE(legacy_id, 999) AS legacy_id
        FROM categories_general
        WHERE year = %s
        ORDER BY legacy_id ASC, name ASC
        """,
        (year,),
    )
    if rows:
        return rows

    return _db_fetchall(
        """
        SELECT DISTINCT category_id AS id, category_name AS name, 999 AS legacy_id
        FROM results
        WHERE year = %s
          AND category_id IS NOT NULL
          AND category_name IS NOT NULL
        ORDER BY name ASC
        """,
        (year,),
    )


@st.cache_data(ttl=1800, show_spinner=False)
def get_team_colors(year: int, category_id: str, category_name: str) -> Dict[str, str]:
    rows = _db_fetchall(
        """
        SELECT
            team_name,
            team_color,
            category_id,
            category_name,
            COALESCE(rider_current, 0) AS rider_current
        FROM TeamRiders
        WHERE year = %s
          AND team_name IS NOT NULL
          AND team_name <> ''
        ORDER BY rider_current DESC
        """,
        (str(year),),
    )
    wanted_category_id = str(category_id or "").strip()
    wanted_category_name = _normalize_category_name(category_name)

    colors: Dict[str, str] = {}
    for row in rows:
        row_category_id = str(row.get("category_id") or "").strip()
        row_category_name = _normalize_category_name(row.get("category_name"))
        if row_category_id != wanted_category_id and row_category_name != wanted_category_name:
            continue

        team_key = _normalize_team_name(row.get("team_name"))
        if not team_key:
            continue
        normalized = _normalize_hex_color(row.get("team_color"))
        if team_key not in colors:
            colors[team_key] = normalized or _fallback_color(team_key)
        elif not _normalize_hex_color(colors.get(team_key)) and normalized:
            colors[team_key] = normalized
    return colors


@st.cache_data(ttl=900, show_spinner=False)
def get_events_for_year(year: int) -> List[Dict[str, Any]]:
    db_events = _db_fetchall(
        """
        SELECT
            id,
            name,
            sponsored_name,
            date_start,
            date_end,
            country_name,
            circuit_name
        FROM events
        WHERE year = %s
          AND COALESCE(test, 0) <> 1
        ORDER BY date_start ASC, id ASC
        """,
        (year,),
    )
    merged: Dict[str, Dict[str, Any]] = {str(event.get("id")): dict(event) for event in db_events if event.get("id")}

    try:
        http = get_http_session()
        seasons = request_json(http, "https://api.motogp.pulselive.com/motogp/v1/results/seasons")
        season = next((row for row in seasons if _safe_int(row.get("year")) == year), None)
        if season and season.get("id"):
            events = request_json(
                http,
                f"https://api.motogp.pulselive.com/motogp/v1/results/events?seasonUuid={season['id']}",
            )
            for row in events:
                if row.get("test"):
                    continue
                event_id = str(row.get("id") or "").strip()
                if not event_id:
                    continue
                api_event = {
                    "id": event_id,
                    "name": row.get("name"),
                    "sponsored_name": row.get("sponsored_name"),
                    "date_start": row.get("date_start"),
                    "date_end": row.get("date_end"),
                    "country_name": (row.get("country") or {}).get("name"),
                    "circuit_name": (row.get("circuit") or {}).get("name"),
                    "short_name": row.get("short_name"),
                    "status": row.get("status"),
                }
                existing = merged.get(event_id, {})
                merged[event_id] = {**existing, **api_event}
    except Exception as exc:
        logging.warning("API events fetch failed for year %s: %s", year, exc)

    events_list = list(merged.values())
    events_list.sort(
        key=lambda item: (
            _as_date(item.get("date_start")) or date.max,
            str(item.get("id") or ""),
        )
    )
    return events_list


@st.cache_data(ttl=300, show_spinner=False)
def get_standings_rows(year: int, category_id: str) -> List[Dict[str, Any]]:
    rows = _db_fetchall(
        """
        SELECT
            position,
            rider_id,
            riders_api_uuid,
            rider_full_name,
            rider_number,
            team_name,
            constructor_name,
            points
        FROM standing_riders
        WHERE year = %s
          AND category_id = %s
        ORDER BY
            CASE
                WHEN CAST(position AS CHAR) REGEXP '^[0-9]+$' THEN CAST(position AS UNSIGNED)
                ELSE 9999
            END ASC,
            points DESC,
            rider_full_name ASC
        """,
        (year, category_id),
    )

    dedup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("rider_id") or row.get("rider_full_name") or "")
        if key and key not in dedup:
            dedup[key] = row
    return list(dedup.values())


@st.cache_data(ttl=300, show_spinner=False)
def get_sessions_for_event(year: int, event_id: str, category_id: str) -> List[Dict[str, Any]]:
    rows = _db_fetchall(
        """
        SELECT
            id AS session_id,
            type AS session_type,
            number AS session_number,
            date AS session_date
        FROM sessions
        WHERE year = %s
          AND event_id = %s
          AND category_id = %s
        ORDER BY session_date ASC, session_number ASC, session_id ASC
        """,
        (year, event_id, category_id),
    )
    if rows:
        return rows

    return _db_fetchall(
        """
        SELECT
            r.session_id,
            MAX(r.session_type) AS session_type,
            MAX(r.session_number) AS session_number,
            MAX(s.date) AS session_date
        FROM results r
        LEFT JOIN sessions s ON s.id = r.session_id
        WHERE r.year = %s
          AND r.event_id = %s
          AND r.category_id = %s
        GROUP BY r.session_id
        ORDER BY session_date ASC, session_number ASC, r.session_id ASC
        """,
        (year, event_id, category_id),
    )


@st.cache_data(ttl=180, show_spinner=False)
def get_lap_times_for_session(session_id: str) -> List[Dict[str, Any]]:
    return _db_fetchall(
        """
        SELECT rider_name, rider_number, lap_number,
               lap_time, lap_time_ms,
               t1, t1_ms, t2, t2_ms, t3, t3_ms, t4, t4_ms,
               speed, is_best_lap
        FROM lap_times
        WHERE session_id = %s
        ORDER BY rider_name, lap_number
        """,
        (session_id,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_sessions_with_laps(year: int, event_id: str, category_id: str) -> List[str]:
    rows = _db_fetchall(
        """
        SELECT DISTINCT lt.session_id
        FROM lap_times lt
        JOIN sessions s ON s.id = lt.session_id
        WHERE lt.year = %s
          AND lt.event_id = %s
          AND lt.category_id = %s
        """,
        (year, event_id, category_id),
    )
    return [str(r["session_id"]) for r in rows]


@st.cache_data(ttl=180, show_spinner=False)
def get_rider_colors_for_session(session_id: str, year: int, category_id: str, category_name: str) -> Dict[str, str]:
    """Returns rider_name -> display hex color using team colors from DB."""
    team_colors = get_team_colors(year, category_id, category_name)
    rows = _db_fetchall(
        "SELECT rider_full_name, team_name FROM results WHERE session_id = %s",
        (session_id,),
    )
    result: Dict[str, str] = {}
    for row in rows:
        name = str(row.get("rider_full_name") or "").strip()
        team = str(row.get("team_name") or "").strip()
        if name:
            result[name] = _team_color(team, team_colors) if team else _fallback_color(name)
    return result


@st.cache_data(ttl=180, show_spinner=False)
def get_session_results(session_id: str) -> List[Dict[str, Any]]:
    return _db_fetchall(
        """
        SELECT
            position,
            rider_id,
            riders_api_uuid,
            rider_full_name,
            rider_number,
            team_name,
            constructor_name,
            total_laps,
            time,
            gap_first,
            average_speed,
            top_speed,
            points,
            status,
            file
        FROM results
        WHERE session_id = %s
        ORDER BY
            CASE
                WHEN CAST(position AS CHAR) REGEXP '^[0-9]+$' THEN CAST(position AS UNSIGNED)
                ELSE 9999
            END ASC,
            rider_full_name ASC
        """,
        (session_id,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_latest_event_with_results(year: int, category_id: str) -> Optional[str]:
    rows = _db_fetchall(
        """
        SELECT
            r.event_id,
            MAX(COALESCE(e.date_end, e.date_start)) AS event_date
        FROM results r
        LEFT JOIN events e
          ON e.id = r.event_id
         AND e.year = r.year
        WHERE r.year = %s
          AND r.category_id = %s
          AND COALESCE(r.event_id, '') <> ''
        GROUP BY r.event_id
        ORDER BY event_date DESC, r.event_id DESC
        LIMIT 1
        """,
        (year, category_id),
    )
    if not rows:
        return None
    return str(rows[0].get("event_id") or "").strip() or None


@st.cache_data(ttl=300, show_spinner=False)
def get_rider_performance(year: int, category_id: str) -> List[Dict[str, Any]]:
    return _db_fetchall(
        """
        SELECT
            rider_id,
            MAX(rider_full_name) AS rider_name,
            SUM(
                CASE
                    WHEN session_type IN %s
                     AND CAST(position AS CHAR) REGEXP '^[0-9]+$'
                     AND CAST(position AS UNSIGNED) = 1
                    THEN 1 ELSE 0
                END
            ) AS wins,
            SUM(
                CASE
                    WHEN session_type IN %s
                     AND CAST(position AS CHAR) REGEXP '^[0-9]+$'
                     AND CAST(position AS UNSIGNED) <= 3
                    THEN 1 ELSE 0
                END
            ) AS podiums,
            AVG(
                CASE
                    WHEN session_type IN %s
                     AND CAST(position AS CHAR) REGEXP '^[0-9]+$'
                    THEN CAST(position AS UNSIGNED)
                    ELSE NULL
                END
            ) AS avg_finish
        FROM results
        WHERE year = %s
          AND category_id = %s
        GROUP BY rider_id
        """,
        (RACE_SESSION_TYPES, RACE_SESSION_TYPES, RACE_SESSION_TYPES, year, category_id),
    )


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def render_colored_bar_chart(
    frame: pd.DataFrame,
    value_col: str,
    title: str,
    top_n: int = 10,
) -> None:
    if frame.empty or value_col not in frame.columns:
        st.info("Chart not available.")
        return

    base = frame[["Rider", "BarColor", value_col]].copy()
    base["Rider"] = base["Rider"].astype(str).str.strip()
    base = base[base["Rider"] != ""]
    base[value_col] = pd.to_numeric(base[value_col], errors="coerce").fillna(0)
    # Keep a single bar per rider (highest value) to avoid duplicated labels/bars.
    base = base.sort_values(value_col, ascending=False).groupby("Rider", as_index=False).first()
    base = base.sort_values(value_col, ascending=False).head(top_n).sort_values(value_col, ascending=True)
    if base.empty:
        st.info("Chart not available.")
        return

    if alt is None:
        st.caption(f"{title} (altair not available, fallback chart)")
        st.bar_chart(base.set_index("Rider")[value_col])
        return

    chart = (
        alt.Chart(base)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X(f"{value_col}:Q", title=value_col),
            y=alt.Y(
                "Rider:N",
                sort=None,
                title="",
                axis=alt.Axis(labelLimit=340, labelOverlap=False),
            ),
            color=alt.Color("BarColor:N", scale=None, legend=None),
            tooltip=[alt.Tooltip("Rider:N"), alt.Tooltip(f"{value_col}:Q")],
        )
        .properties(title=title, height=max(280, len(base) * 24))
    )
    st.altair_chart(chart, use_container_width=True)


def style_rows_by_team(frame: pd.DataFrame, team_colors: Optional[pd.Series] = None):
    normalized_colors: Optional[pd.Series] = None
    if team_colors is not None:
        normalized_colors = team_colors.reindex(frame.index)

    def _row_style(row: pd.Series) -> List[str]:
        color = ""
        if normalized_colors is not None:
            color = _normalize_hex_color(normalized_colors.get(row.name))
        if not color:
            color = _normalize_hex_color(row.get("TeamColor"))
        if not color:
            return [""] * len(row)
        r, g, b = _hex_to_rgb(color)
        style = f"background-color: rgba({r}, {g}, {b}, 0.14);"
        return [style] * len(row)

    return frame.style.apply(_row_style, axis=1)


def _section_header(title: str) -> None:
    """Render a styled section header with MotoGP red accent."""
    st.markdown(
        f'<div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.1em;color:{MOTOGP_RED};border-bottom:1px solid {MOTOGP_RED}33;'
        f'padding-bottom:4px;margin-top:1.2rem;margin-bottom:0.6rem;">{title}</div>',
        unsafe_allow_html=True,
    )


def _pos_badge(pos: int) -> str:
    """Return HTML badge for a position number."""
    if pos == 1:
        bg, fg = GOLD, "#111"
    elif pos == 2:
        bg, fg = SILVER, "#111"
    elif pos == 3:
        bg, fg = BRONZE, "#fff"
    else:
        bg, fg = "#333", "#eee"
    return (
        f'<span style="display:inline-block;min-width:26px;text-align:center;'
        f'background:{bg};color:{fg};border-radius:4px;padding:1px 6px;'
        f'font-size:0.8rem;font-weight:700;">{pos}</span>'
    )


# ---------------------------------------------------------------------------
# Tab renderers
# ---------------------------------------------------------------------------

def render_overview_tab(year: int, events: Sequence[Dict[str, Any]]) -> None:
    _section_header("Season Calendar")
    today = date.today()

    finished: List[Dict[str, Any]] = []
    upcoming: List[Dict[str, Any]] = []
    timeline_rows: List[Dict[str, Any]] = []

    for event in events:
        start_date = _as_date(event.get("date_start"))
        end_date = _as_date(event.get("date_end"))
        status = str(event.get("status") or "").upper()

        is_finished = status == "FINISHED" or (end_date is not None and end_date < today)
        is_upcoming = status == "NOT-STARTED" or (start_date is not None and start_date >= today)

        if is_finished:
            finished.append(event)
        elif is_upcoming:
            upcoming.append(event)
        else:
            finished.append(event)

        timeline_rows.append(
            {
                "Start": start_date.isoformat() if start_date else "",
                "End": end_date.isoformat() if end_date else "",
                "Event": _event_title(event),
                "Country": event.get("country_name") or "",
                "Circuit": event.get("circuit_name") or "",
                "Status": status or ("FINISHED" if is_finished else "NOT-STARTED"),
            }
        )

    next_round = sorted(upcoming, key=lambda e: _as_date(e.get("date_start")) or date.max)[0] if upcoming else None
    last_round = sorted(finished, key=lambda e: _as_date(e.get("date_end")) or date.min, reverse=True)[0] if finished else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Completed Rounds", len(finished))
    col2.metric("Upcoming Rounds", len(upcoming))
    col3.metric("Last Round", _event_title(last_round) if last_round else "-")
    col4.metric("Next Round", _event_title(next_round) if next_round else "-")

    timeline_df = pd.DataFrame(timeline_rows)
    if timeline_df.empty:
        st.info(f"No events found in DB for {year}.")
        return

    timeline_df = timeline_df.sort_values("Start")

    _section_header("All Rounds")
    st.dataframe(timeline_df, hide_index=True, use_container_width=True)

    timeline_df["Day"] = pd.to_datetime(timeline_df["Start"], errors="coerce")
    monthly_df = timeline_df.dropna(subset=["Day"]).copy()
    if not monthly_df.empty and alt is not None:
        _section_header("Rounds by Month")
        per_month = monthly_df.groupby(monthly_df["Day"].dt.to_period("M")).size().reset_index()
        per_month.columns = ["Month", "Rounds"]
        per_month["Month"] = per_month["Month"].astype(str)
        bar = (
            alt.Chart(per_month)
            .mark_bar(color=MOTOGP_RED, cornerRadiusEnd=4)
            .encode(
                x=alt.X("Month:N", title="Month"),
                y=alt.Y("Rounds:Q", title="Number of rounds"),
                tooltip=["Month:N", "Rounds:Q"],
            )
            .properties(height=220)
        )
        st.altair_chart(bar, use_container_width=True)
    elif not monthly_df.empty:
        per_month = monthly_df.groupby(monthly_df["Day"].dt.to_period("M")).size()
        per_month.index = per_month.index.astype(str)
        st.caption("Rounds by month")
        st.bar_chart(per_month)

    st.caption("Data source: local MySQL database.")


def render_standings_tab(year: int, category_id: str, category_name: str) -> pd.DataFrame:
    _section_header("Championship Standings")
    rows = get_standings_rows(year, category_id)
    if not rows:
        st.warning("Standings are currently unavailable for this selection.")
        return pd.DataFrame()

    team_colors = get_team_colors(year, category_id, category_name)
    mapped_rows: List[Dict[str, Any]] = []
    for item in rows:
        points = _safe_int(item.get("points"))
        rider_id = item.get("rider_id") or ""
        rider_name = item.get("rider_full_name") or ""
        team_name = item.get("team_name") or ""
        team_color = _team_color(team_name, team_colors)
        mapped_rows.append(
            {
                "Pos": _safe_int(item.get("position"), default=9999),
                "Rider": rider_name,
                "#": _safe_int(item.get("rider_number")),
                "Team": team_name,
                "Constructor": item.get("constructor_name") or "",
                "Points": points,
                "Power Index": points,
                "Consistency": 0.0,
                "Wins": 0,
                "Podiums": 0,
                "Rider ID": rider_id,
                "TeamColor": team_color,
                "BarColor": team_color,
            }
        )

    df = pd.DataFrame(mapped_rows)
    if df.empty:
        st.warning("Standings are currently unavailable for this selection.")
        return pd.DataFrame()

    perf_rows = get_rider_performance(year, category_id)
    perf_map = {str(row.get("rider_id")): row for row in perf_rows}

    for idx, row in df.iterrows():
        rider_id = str(row.get("Rider ID") or "")
        perf = perf_map.get(rider_id) or {}
        wins = _safe_int(perf.get("wins"))
        podiums = _safe_int(perf.get("podiums"))
        avg_finish = _safe_float(perf.get("avg_finish"), default=0.0)
        consistency = 0.0
        if avg_finish > 0:
            consistency = max(0.0, 100.0 - (avg_finish - 1.0) * 12.0)
        df.at[idx, "Power Index"] = int(_safe_int(row.get("Points")) + wins * 12 + podiums * 4)
        df.at[idx, "Consistency"] = round(consistency, 1)
        df.at[idx, "Wins"] = wins
        df.at[idx, "Podiums"] = podiums

    df = df.sort_values(["Pos", "Points"], ascending=[True, False]).reset_index(drop=True)

    leader = df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Leader", leader["Rider"])
    col2.metric("Leader Points", int(leader["Points"]))
    col3.metric("Power Index", int(leader["Power Index"]))
    col4.metric("Riders in Standings", int(len(df)))

    # Standings table with position badge rendered via HTML
    _section_header("Full Classification")

    # Build a display df without internal columns
    display_df = df.drop(columns=["Rider ID", "BarColor", "TeamColor"], errors="ignore")

    # Render position badge via styling
    def _highlight_pos(row: pd.Series) -> List[str]:
        pos = _safe_int(row.get("Pos"), default=9999)
        team_color = ""
        if "TeamColor" in df.columns:
            tc_series = df.loc[df["Rider"] == row.get("Rider"), "TeamColor"]
            if not tc_series.empty:
                team_color = _normalize_hex_color(tc_series.iloc[0])

        styles: List[str] = []
        for col_name in row.index:
            if col_name == "Pos":
                if pos == 1:
                    styles.append(f"background-color: {GOLD}33; font-weight: 800; color: #b8860b;")
                elif pos == 2:
                    styles.append(f"background-color: {SILVER}33; font-weight: 800; color: #777;")
                elif pos == 3:
                    styles.append(f"background-color: {BRONZE}33; font-weight: 800; color: #a0522d;")
                else:
                    styles.append("font-weight: 600;")
            elif team_color:
                r, g, b = _hex_to_rgb(team_color)
                styles.append(f"background-color: rgba({r},{g},{b},0.10);")
            else:
                styles.append("")
        return styles

    st.dataframe(
        display_df.style.apply(_highlight_pos, axis=1),
        hide_index=True,
        use_container_width=True,
    )

    _section_header("Performance Charts")
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        render_colored_bar_chart(df, value_col="Points", title="Top 10 — Points", top_n=10)
    with chart_col2:
        render_colored_bar_chart(df, value_col="Power Index", title="Top 10 — Power Index", top_n=10)

    return df


def render_results_tab(year: int, category_id: str, category_name: str, events: Sequence[Dict[str, Any]]) -> None:
    _section_header("Session Results")
    if not events:
        st.info("No events available.")
        return

    ordered_events = sorted(
        [event for event in events if event.get("id")],
        key=lambda event: _as_date(event.get("date_end")) or date.min,
        reverse=True,
    )
    if not ordered_events:
        st.info("No rounds with valid IDs are available.")
        return

    latest_event_with_results = get_latest_event_with_results(year, category_id)
    default_event_index = 0
    if latest_event_with_results:
        matched_index = next(
            (
                idx
                for idx, event in enumerate(ordered_events)
                if str(event.get("id") or "") == latest_event_with_results
            ),
            None,
        )
        if matched_index is not None:
            default_event_index = matched_index
    else:
        today = date.today()
        latest_completed_index = next(
            (
                idx
                for idx, event in enumerate(ordered_events)
                if str(event.get("status") or "").strip().upper() == "FINISHED"
                or (
                    (_as_date(event.get("date_end")) or _as_date(event.get("date_start")))
                    and (
                        (_as_date(event.get("date_end")) or _as_date(event.get("date_start"))) <= today
                    )
                )
            ),
            None,
        )
        if latest_completed_index is not None:
            default_event_index = latest_completed_index

    selected_event = st.selectbox(
        "Select Round / Circuit",
        ordered_events,
        index=default_event_index,
        format_func=lambda event: (
            f"{_as_date(event.get('date_start')) or '-'} | "
            f"{event.get('circuit_name') or 'Unknown circuit'} | "
            f"{_event_title(event)}"
        ),
    )
    st.caption(
        f"Selected: **{_event_title(selected_event)}** "
        f"({_as_date(selected_event.get('date_start')) or '-'} – {_as_date(selected_event.get('date_end')) or '-'})"
    )

    sessions = get_sessions_for_event(year, str(selected_event["id"]), category_id)
    if not sessions:
        st.info("No sessions are available for this round/category.")
        return

    sessions_sorted = sorted(
        [session for session in sessions if session.get("session_id")],
        key=lambda session: (
            _as_date(session.get("session_date")) or date.min,
            _safe_int(session.get("session_number"), default=999),
            str(session.get("session_id")),
        ),
    )
    if not sessions_sorted:
        st.info("No valid sessions are available for this round/category.")
        return

    default_session_index = max(len(sessions_sorted) - 1, 0)
    selected_session = st.selectbox(
        "Select Session",
        sessions_sorted,
        index=default_session_index,
        format_func=_session_label,
    )

    rows = get_session_results(str(selected_session["session_id"]))
    if not rows:
        st.warning("Session classification is not available yet.")
        return

    team_colors = get_team_colors(year, category_id, category_name)
    result_rows: List[Dict[str, Any]] = []
    for item in rows:
        rider_id = item.get("rider_id") or ""
        rider_name = item.get("rider_full_name") or ""
        team_name = item.get("team_name") or ""
        team_color = _team_color(team_name, team_colors)
        result_rows.append(
            {
                "Pos": _safe_int(item.get("position"), default=9999),
                "Rider": rider_name,
                "#": _safe_int(item.get("rider_number")),
                "Team": team_name,
                "Constructor": item.get("constructor_name") or "",
                "Laps": _safe_int(item.get("total_laps")),
                "Time": item.get("time") or "",
                "Gap": item.get("gap_first") or "",
                "Avg Speed": _safe_float(item.get("average_speed")),
                "Top Speed": _safe_float(item.get("top_speed")),
                "Points": _safe_int(item.get("points")),
                "Status": item.get("status") or "",
                "File URL": item.get("file") or "",
                "Rider ID": rider_id,
                "TeamColor": team_color,
                "BarColor": team_color,
            }
        )

    results_df = pd.DataFrame(result_rows).sort_values("Pos").reset_index(drop=True)
    if results_df.empty:
        st.warning("Session classification is not available yet.")
        return

    winner = results_df.iloc[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("Winner", winner["Rider"])
    col2.metric("Session", _session_label(selected_session))
    col3.metric("Winner Points", int(winner["Points"]))

    file_url = ""
    for value in results_df["File URL"].tolist():
        if str(value).strip():
            file_url = str(value).strip()
            break
    if file_url:
        st.link_button("Open Official Session PDF", file_url)

    _section_header("Classification")
    results_display_df = results_df.drop(columns=["File URL", "Rider ID", "BarColor", "TeamColor"], errors="ignore")
    st.dataframe(
        style_rows_by_team(results_display_df, results_df.get("TeamColor")),
        hide_index=True,
        use_container_width=True,
    )

    speed_df = results_df[results_df["Top Speed"] > 0].copy()
    if not speed_df.empty:
        _section_header("Top Speed Trap")
        render_colored_bar_chart(speed_df, value_col="Top Speed", title="Top speed by rider (km/h)", top_n=10)


def render_lap_times_tab(year: int, category_id: str, category_name: str, events: Sequence[Dict[str, Any]]) -> None:
    _section_header("Lap Times Analysis")

    if not events:
        st.info("No events available.")
        return

    ordered_events = sorted(
        [e for e in events if e.get("id")],
        key=lambda e: _as_date(e.get("date_end")) or date.min,
        reverse=True,
    )

    col_ev, col_sess = st.columns([3, 1])
    with col_ev:
        selected_event = st.selectbox(
            "Round",
            ordered_events,
            key="lt_event",
            format_func=lambda e: (
                f"{_as_date(e.get('date_start')) or '-'} | "
                f"{e.get('circuit_name') or 'Unknown'} | "
                f"{_event_title(e)}"
            ),
        )

    sessions = get_sessions_for_event(year, str(selected_event["id"]), category_id)
    sessions_sorted = sorted(
        [s for s in sessions if s.get("session_id")],
        key=lambda s: (
            _as_date(s.get("session_date")) or date.min,
            _safe_int(s.get("session_number"), 999),
        ),
    )

    sessions_with_laps = set(get_sessions_with_laps(year, str(selected_event["id"]), category_id))
    sessions_with_data = [s for s in sessions_sorted if str(s["session_id"]) in sessions_with_laps]

    if not sessions_with_data:
        st.info("No lap time data available for this round yet. Run `lap_times.py` to import.")
        return

    with col_sess:
        selected_session = st.selectbox(
            "Session",
            sessions_with_data,
            key="lt_session",
            index=len(sessions_with_data) - 1,
            format_func=_session_label,
        )

    session_id = str(selected_session["session_id"])
    laps = get_lap_times_for_session(session_id)
    if not laps:
        st.info("No lap data found for this session.")
        return

    df = pd.DataFrame(laps)
    df["lap_time_ms"] = pd.to_numeric(df["lap_time_ms"], errors="coerce")
    df["lap_time_s"] = df["lap_time_ms"] / 1000
    df["lap_number"] = pd.to_numeric(df["lap_number"], errors="coerce")

    # Rider colour map: prefer DB team colours, fall back to deterministic palette
    rider_color_map = get_rider_colors_for_session(session_id, year, category_id, category_name)

    # Rider filter
    riders_all = sorted(df["rider_name"].dropna().unique().tolist())
    selected_riders = st.multiselect(
        "Filter riders (empty = all)",
        riders_all,
        default=[],
        key="lt_riders",
        placeholder="All riders",
    )
    if selected_riders:
        df = df[df["rider_name"].isin(selected_riders)]

    if df.empty:
        st.info("No data for selected riders.")
        return

    # --- KPI metrics ---
    valid = df[df["lap_time_ms"].notna() & (df["lap_time_ms"] > 0)]
    best_row = valid.loc[valid["lap_time_ms"].idxmin()] if not valid.empty else None
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Laps", len(df))
    m2.metric("Riders", df["rider_name"].nunique())
    m3.metric("Fastest Lap", best_row["lap_time"] if best_row is not None else "-")
    m4.metric("Fastest Rider", str(best_row["rider_name"]) if best_row is not None else "-")

    # Resolve colours for currently filtered riders
    def _rider_color(name: str) -> str:
        c = rider_color_map.get(str(name), "")
        return c if c else _fallback_color(str(name))

    rider_list_filtered = df["rider_name"].dropna().unique().tolist()
    color_range = [_rider_color(r) for r in rider_list_filtered]

    # -----------------------------------------------------------------------
    # RACE TRACE (gap to leader per lap)
    # -----------------------------------------------------------------------
    _section_header("Race Trace — Gap to Leader")

    median_ms = valid["lap_time_ms"].median() if not valid.empty else None
    if median_ms and median_ms > 0:
        df_clean = valid[valid["lap_time_ms"] <= median_ms * 1.15].copy()
    else:
        df_clean = valid.copy()

    if not df_clean.empty and df_clean["lap_number"].notna().any():
        df_clean = df_clean.sort_values(["rider_name", "lap_number"])
        df_clean["cumulative_ms"] = df_clean.groupby("rider_name")["lap_time_ms"].cumsum()
        leader = (
            df_clean.groupby("lap_number")["cumulative_ms"]
            .min()
            .rename("leader_ms")
        )
        df_clean = df_clean.join(leader, on="lap_number")
        df_clean["gap_s"] = (df_clean["cumulative_ms"] - df_clean["leader_ms"]) / 1000

        if alt is not None:
            trace_riders = df_clean["rider_name"].dropna().unique().tolist()
            trace_colors = [_rider_color(r) for r in trace_riders]
            trace_chart = (
                alt.Chart(df_clean)
                .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30, filled=True))
                .encode(
                    x=alt.X("lap_number:Q", title="Lap"),
                    y=alt.Y(
                        "gap_s:Q",
                        title="Gap to leader (s)",
                        scale=alt.Scale(reverse=True),
                        axis=alt.Axis(format=".1f"),
                    ),
                    color=alt.Color(
                        "rider_name:N",
                        title="Rider",
                        scale=alt.Scale(domain=trace_riders, range=trace_colors),
                    ),
                    tooltip=[
                        alt.Tooltip("rider_name:N", title="Rider"),
                        alt.Tooltip("lap_number:Q", title="Lap"),
                        alt.Tooltip("gap_s:Q", title="Gap (s)", format=".3f"),
                        alt.Tooltip("lap_time_s:Q", title="Lap time (s)", format=".3f"),
                    ],
                )
                .properties(height=400)
                .interactive()
            )
            st.altair_chart(trace_chart, use_container_width=True)
        else:
            pivot = df_clean.pivot_table(index="lap_number", columns="rider_name", values="gap_s")
            st.line_chart(pivot)
    else:
        st.info("Not enough laps to compute race trace.")

    # -----------------------------------------------------------------------
    # PACE DISTRIBUTION (box plot)
    # -----------------------------------------------------------------------
    _section_header("Pace Distribution")

    if not valid.empty and alt is not None:
        box_median = valid["lap_time_ms"].median()
        clean_for_pace = valid[valid["lap_time_ms"] <= box_median * 1.07].copy()
        clean_for_pace["lap_time_s"] = clean_for_pace["lap_time_ms"] / 1000

        if not clean_for_pace.empty:
            # Compute median per rider to sort the axis
            rider_medians = (
                clean_for_pace.groupby("rider_name")["lap_time_s"]
                .median()
                .sort_values()
            )
            rider_order = rider_medians.index.tolist()
            pace_riders = clean_for_pace["rider_name"].dropna().unique().tolist()
            pace_colors = [_rider_color(r) for r in pace_riders]

            box_chart = (
                alt.Chart(clean_for_pace)
                .mark_boxplot(extent="min-max", size=14)
                .encode(
                    x=alt.X(
                        "lap_time_s:Q",
                        title="Lap time (s)",
                        scale=alt.Scale(zero=False),
                    ),
                    y=alt.Y(
                        "rider_name:N",
                        title="",
                        sort=rider_order,
                        axis=alt.Axis(labelLimit=300),
                    ),
                    color=alt.Color(
                        "rider_name:N",
                        scale=alt.Scale(domain=pace_riders, range=pace_colors),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("rider_name:N", title="Rider"),
                        alt.Tooltip("lap_time_s:Q", title="Lap time (s)", format=".3f"),
                    ],
                )
                .properties(height=max(320, len(rider_order) * 22))
            )
            st.altair_chart(box_chart, use_container_width=True)
        else:
            st.info("Not enough clean laps for pace distribution.")
    elif alt is None:
        st.info("Install altair for pace distribution chart.")

    # -----------------------------------------------------------------------
    # BEST LAP COMPARISON
    # -----------------------------------------------------------------------
    _section_header("Best Lap Ranking")

    best_laps = df[df["is_best_lap"] == 1].copy()
    if best_laps.empty:
        best_laps = df.loc[
            df[df["lap_time_ms"].notna() & (df["lap_time_ms"] > 0)]
            .groupby("rider_name")["lap_time_ms"]
            .idxmin()
        ].copy()
    best_laps = best_laps.sort_values("lap_time_ms").reset_index(drop=True)
    best_laps["BarColor"] = best_laps["rider_name"].apply(lambda r: _rider_color(str(r)))
    best_laps["Rider"] = best_laps["rider_name"]
    best_laps["Best Lap (s)"] = best_laps["lap_time_ms"] / 1000

    if alt is not None and not best_laps.empty:
        bl_sorted = best_laps.sort_values("Best Lap (s)", ascending=True).head(30)
        bl_chart = (
            alt.Chart(bl_sorted)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("Best Lap (s):Q", title="Best lap time (s)", scale=alt.Scale(zero=False)),
                y=alt.Y("Rider:N", sort=list(bl_sorted["Rider"]), title=""),
                color=alt.Color("BarColor:N", scale=None, legend=None),
                tooltip=[
                    alt.Tooltip("Rider:N"),
                    alt.Tooltip("Best Lap (s):Q", format=".3f"),
                ],
            )
            .properties(height=max(280, len(bl_sorted) * 22), title="Best lap per rider")
        )
        st.altair_chart(bl_chart, use_container_width=True)
    else:
        render_colored_bar_chart(best_laps, value_col="Best Lap (s)", title="Best lap ranking", top_n=30)

    # -----------------------------------------------------------------------
    # SECTOR TIMES (T1–T4 grouped bar)
    # -----------------------------------------------------------------------
    sector_df = best_laps.dropna(subset=["t1_ms"]).copy()
    if not sector_df.empty and alt is not None:
        _section_header("Sector Times — Best Lap")
        melted = sector_df.melt(
            id_vars="rider_name",
            value_vars=["t1_ms", "t2_ms", "t3_ms", "t4_ms"],
            var_name="sector",
            value_name="time_ms",
        )
        melted = melted.dropna(subset=["time_ms"])
        melted["time_s"] = melted["time_ms"] / 1000
        melted["sector"] = melted["sector"].str.replace("_ms", "").str.upper()
        melted["rider_name"] = melted["rider_name"].astype(str)

        sector_chart = (
            alt.Chart(melted)
            .mark_bar()
            .encode(
                x=alt.X("time_s:Q", title="Sector time (s)"),
                y=alt.Y("rider_name:N", title="", sort="-x"),
                color=alt.Color(
                    "sector:N",
                    title="Sector",
                    scale=alt.Scale(
                        domain=["T1", "T2", "T3", "T4"],
                        range=["#E8002D", "#FF6B35", "#FFC72C", "#44B8A8"],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("rider_name:N", title="Rider"),
                    alt.Tooltip("sector:N", title="Sector"),
                    alt.Tooltip("time_s:Q", title="Time (s)", format=".3f"),
                ],
            )
            .properties(height=max(300, len(sector_df) * 22), title="Sector breakdown (best lap)")
        )
        st.altair_chart(sector_chart, use_container_width=True)

    # -----------------------------------------------------------------------
    # FULL LAP TABLE
    # -----------------------------------------------------------------------
    with st.expander("Full Lap Data Table"):
        show_cols = ["rider_name", "lap_number", "lap_time", "t1", "t2", "t3", "t4", "speed", "is_best_lap"]
        available = [c for c in show_cols if c in df.columns]
        st.dataframe(
            df[available].rename(
                columns={
                    "rider_name": "Rider",
                    "lap_number": "Lap",
                    "lap_time": "Time",
                    "is_best_lap": "Best",
                }
            ),
            hide_index=True,
            use_container_width=True,
        )


def render_stats_lab(standings_df: pd.DataFrame) -> None:
    _section_header("Stats Lab")
    st.caption("Experimental metrics calculated from local DB data.")
    if standings_df.empty:
        st.info("Standings data is required to compute these stats.")
        return

    col1, col2 = st.columns(2)
    with col1:
        render_colored_bar_chart(standings_df, value_col="Power Index", title="Power Index — Top 10", top_n=10)
    with col2:
        render_colored_bar_chart(standings_df, value_col="Consistency", title="Consistency — Top 10", top_n=10)

    col3, col4 = st.columns(2)
    with col3:
        render_colored_bar_chart(standings_df, value_col="Wins", title="Wins — Top 10", top_n=10)
    with col4:
        render_colored_bar_chart(standings_df, value_col="Podiums", title="Podiums — Top 10", top_n=10)

    scatter_df = standings_df.copy()
    if not scatter_df.empty and alt is not None:
        _section_header("Points vs Consistency")
        scatter_chart = (
            alt.Chart(scatter_df)
            .mark_circle(size=90, opacity=0.85)
            .encode(
                x=alt.X("Consistency:Q", title="Consistency score"),
                y=alt.Y("Points:Q", title="Championship points"),
                color=alt.Color("BarColor:N", scale=None, legend=None),
                size=alt.Size("Wins:Q", legend=None, scale=alt.Scale(range=[60, 400])),
                tooltip=[
                    alt.Tooltip("Rider:N"),
                    alt.Tooltip("Points:Q"),
                    alt.Tooltip("Consistency:Q"),
                    alt.Tooltip("Wins:Q"),
                    alt.Tooltip("Podiums:Q"),
                ],
            )
            .properties(title="Bubble size = Wins", height=380)
        )
        st.altair_chart(scatter_chart, use_container_width=True)

    st.caption("Charts are calculated from synchronized local DB data.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    st.set_page_config(
        page_title="MotoGP Race Hub",
        page_icon="\U0001f3c1",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Professional dark-accent motorsport theme injected on top of default Streamlit
    st.markdown(
        """
        <style>
        .block-container {padding-top: 0.8rem; padding-bottom: 2rem;}

        /* Metric cards */
        [data-testid="metric-container"] {
            border: 1px solid #e8e8e8;
            border-radius: 10px;
            padding: 12px 16px;
            background: rgba(232, 0, 45, 0.04);
            border-left: 3px solid #E8002D;
        }
        [data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
            font-weight: 800 !important;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.7rem !important;
            font-weight: 700 !important;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #888 !important;
        }

        /* Tab bar */
        .stTabs [data-baseweb="tab"] {
            font-weight: 600;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
        }
        .stTabs [aria-selected="true"] {
            color: #E8002D !important;
            border-bottom: 3px solid #E8002D !important;
        }

        /* Sidebar header */
        [data-testid="stSidebarContent"] h2,
        [data-testid="stSidebarContent"] h3 {
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: #E8002D;
        }

        /* App title accent */
        .mgp-title {
            font-size: 1.9rem;
            font-weight: 900;
            letter-spacing: -0.02em;
        }
        .mgp-title span {
            color: #E8002D;
        }
        .mgp-subtitle {
            color: #888;
            font-size: 0.88rem;
            margin-top: -6px;
            margin-bottom: 0.6rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="mgp-title">MotoGP <span>Race Hub</span></div>'
        '<div class="mgp-subtitle">Results, standings & telemetry analytics from your local database</div>',
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown(
            f'<div style="color:{MOTOGP_RED};font-weight:900;font-size:1.1rem;'
            f'letter-spacing:0.04em;margin-bottom:0.5rem;">CONTROLS</div>',
            unsafe_allow_html=True,
        )
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.divider()

        years = get_available_years()
        if not years:
            st.error("No season data available in database.")
            st.stop()

        default_year = 2026 if 2026 in years else years[0]
        selected_year = st.selectbox("Season", years, index=years.index(default_year))

        categories = get_categories_for_year(selected_year)
        if not categories:
            st.error("No categories available for this season in database.")
            st.stop()
        default_category_index = 0
        for idx, row in enumerate(categories):
            name = str(row.get("name") or "").lower()
            legacy = _safe_int(row.get("legacy_id"), default=999)
            if "motogp" in name or legacy == 3:
                default_category_index = idx
                break
        selected_category = st.selectbox(
            "Category",
            categories,
            index=default_category_index,
            format_func=lambda row: row.get("name", "Unknown"),
        )
        selected_category_id = str(selected_category["id"])
        selected_category_name = str(selected_category.get("name") or "")

        st.divider()
        st.caption(f"DB source: local MySQL\n\nSeason: **{selected_year}** | {selected_category_name}")

    events = get_events_for_year(selected_year)

    tab_overview, tab_standings, tab_results, tab_lap_times, tab_stats = st.tabs(
        ["\U0001f4c5  Overview", "\U0001f3c6  Standings", "\U0001f3c1  Results", "⏱️  Lap Times", "\U0001f9ea  Stats Lab"]
    )

    with tab_overview:
        render_overview_tab(selected_year, events)

    with tab_standings:
        standings_df = render_standings_tab(selected_year, selected_category_id, selected_category_name)

    with tab_results:
        render_results_tab(selected_year, selected_category_id, selected_category_name, events)

    with tab_lap_times:
        render_lap_times_tab(selected_year, selected_category_id, selected_category_name, events)

    with tab_stats:
        render_stats_lab(standings_df if "standings_df" in locals() else pd.DataFrame())


if __name__ == "__main__":
    main()
