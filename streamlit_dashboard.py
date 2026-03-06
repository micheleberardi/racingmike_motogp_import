import logging
import math
import re
from hashlib import md5
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import altair as alt
import pandas as pd
import streamlit as st

from runtime import get_db_connection

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


def _rider_color(rider_id: Any, rider_name: Any, color_map: Dict[str, str]) -> str:
    rider_key = str(rider_id or "").strip()
    if rider_key and rider_key in color_map:
        return color_map[rider_key]
    fallback_key = rider_key or str(rider_name or "unknown")
    return _fallback_color(fallback_key)


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
def get_rider_colors(year: int) -> Dict[str, str]:
    rows = _db_fetchall(
        """
        SELECT
            rider_id,
            team_color,
            COALESCE(rider_current, 0) AS rider_current
        FROM TeamRiders
        WHERE year = %s
          AND rider_id IS NOT NULL
          AND rider_id <> ''
        ORDER BY rider_current DESC
        """,
        (str(year),),
    )
    colors: Dict[str, str] = {}
    for row in rows:
        rider_id = str(row.get("rider_id") or "").strip()
        if not rider_id:
            continue
        normalized = _normalize_hex_color(row.get("team_color"))
        if rider_id not in colors:
            colors[rider_id] = normalized or _fallback_color(rider_id)
        elif not _normalize_hex_color(colors.get(rider_id)) and normalized:
            colors[rider_id] = normalized
    return colors


@st.cache_data(ttl=900, show_spinner=False)
def get_events_for_year(year: int) -> List[Dict[str, Any]]:
    return _db_fetchall(
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


@st.cache_data(ttl=300, show_spinner=False)
def get_standings_rows(year: int, category_id: str) -> List[Dict[str, Any]]:
    rows = _db_fetchall(
        """
        SELECT
            position,
            rider_id,
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
def get_session_results(session_id: str) -> List[Dict[str, Any]]:
    return _db_fetchall(
        """
        SELECT
            position,
            rider_id,
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
    base[value_col] = pd.to_numeric(base[value_col], errors="coerce").fillna(0)
    base = base.sort_values(value_col, ascending=False).head(top_n).sort_values(value_col, ascending=True)
    if base.empty:
        st.info("Chart not available.")
        return

    chart = (
        alt.Chart(base)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X(f"{value_col}:Q", title=value_col),
            y=alt.Y("Rider:N", sort=None, title=""),
            color=alt.Color("BarColor:N", scale=None, legend=None),
            tooltip=[alt.Tooltip("Rider:N"), alt.Tooltip(f"{value_col}:Q")],
        )
        .properties(title=title, height=max(280, len(base) * 24))
    )
    st.altair_chart(chart, use_container_width=True)


def render_overview_tab(year: int, events: Sequence[Dict[str, Any]]) -> None:
    st.subheader("Season overview")
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
    col1.metric("Completed rounds", len(finished))
    col2.metric("Upcoming rounds", len(upcoming))
    col3.metric("Last round", _event_title(last_round) if last_round else "-")
    col4.metric("Next round", _event_title(next_round) if next_round else "-")

    timeline_df = pd.DataFrame(timeline_rows)
    if timeline_df.empty:
        st.info(f"No events found in DB for {year}.")
        return

    timeline_df = timeline_df.sort_values("Start")
    st.dataframe(timeline_df, hide_index=True, use_container_width=True)

    timeline_df["Day"] = pd.to_datetime(timeline_df["Start"], errors="coerce")
    monthly_df = timeline_df.dropna(subset=["Day"]).copy()
    if not monthly_df.empty:
        per_month = monthly_df.groupby(monthly_df["Day"].dt.to_period("M")).size()
        per_month.index = per_month.index.astype(str)
        st.caption("Rounds by month")
        st.bar_chart(per_month)

    st.caption("Data source: local MySQL database.")


def render_standings_tab(year: int, category_id: str) -> pd.DataFrame:
    st.subheader("Championship standings")
    rows = get_standings_rows(year, category_id)
    if not rows:
        st.warning("Standings are currently unavailable for this selection.")
        return pd.DataFrame()

    rider_colors = get_rider_colors(year)
    mapped_rows: List[Dict[str, Any]] = []
    for item in rows:
        points = _safe_int(item.get("points"))
        rider_id = item.get("rider_id") or ""
        rider_name = item.get("rider_full_name") or ""
        mapped_rows.append(
            {
                "Pos": _safe_int(item.get("position"), default=9999),
                "Rider": rider_name,
                "#": _safe_int(item.get("rider_number")),
                "Team": item.get("team_name") or "",
                "Constructor": item.get("constructor_name") or "",
                "Points": points,
                "Power Index": points,
                "Consistency": 0.0,
                "Wins": 0,
                "Podiums": 0,
                "Rider ID": rider_id,
                "BarColor": _rider_color(rider_id, rider_name, rider_colors),
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
    col2.metric("Leader points", int(leader["Points"]))
    col3.metric("Leader power index", int(leader["Power Index"]))
    col4.metric("Riders in standings", int(len(df)))

    display_df = df.drop(columns=["Rider ID", "BarColor"], errors="ignore")
    st.dataframe(display_df, hide_index=True, use_container_width=True)

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        render_colored_bar_chart(df, value_col="Points", title="Top 10 by points", top_n=10)
    with chart_col2:
        render_colored_bar_chart(df, value_col="Power Index", title="Top 10 by power index", top_n=10)

    return df


def render_results_tab(year: int, category_id: str, events: Sequence[Dict[str, Any]]) -> None:
    st.subheader("Session results")
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

    selected_event = st.selectbox(
        "Select round",
        ordered_events,
        format_func=lambda event: f"{_as_date(event.get('date_start')) or ''} - {_event_title(event)}",
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

    selected_session = st.selectbox(
        "Select session",
        sessions_sorted,
        format_func=lambda session: (
            f"{session.get('session_date') or '-'} | "
            f"{session.get('session_type') or 'UNKNOWN'} | "
            f"N{_safe_int(session.get('session_number'), default=0)}"
        ),
    )

    rows = get_session_results(str(selected_session["session_id"]))
    if not rows:
        st.warning("Session classification is not available yet.")
        return

    rider_colors = get_rider_colors(year)
    result_rows: List[Dict[str, Any]] = []
    for item in rows:
        rider_id = item.get("rider_id") or ""
        rider_name = item.get("rider_full_name") or ""
        result_rows.append(
            {
                "Pos": _safe_int(item.get("position"), default=9999),
                "Rider": rider_name,
                "#": _safe_int(item.get("rider_number")),
                "Team": item.get("team_name") or "",
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
                "BarColor": _rider_color(rider_id, rider_name, rider_colors),
            }
        )

    results_df = pd.DataFrame(result_rows).sort_values("Pos").reset_index(drop=True)
    if results_df.empty:
        st.warning("Session classification is not available yet.")
        return

    winner = results_df.iloc[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("Winner", winner["Rider"])
    col2.metric("Session", selected_session.get("session_type") or "-")
    col3.metric("Winner points", int(winner["Points"]))

    file_url = ""
    for value in results_df["File URL"].tolist():
        if str(value).strip():
            file_url = str(value).strip()
            break
    if file_url:
        st.link_button("Open official session PDF", file_url)

    st.dataframe(
        results_df.drop(columns=["File URL", "Rider ID", "BarColor"], errors="ignore"),
        hide_index=True,
        use_container_width=True,
    )

    speed_df = results_df[results_df["Top Speed"] > 0].copy()
    if not speed_df.empty:
        render_colored_bar_chart(speed_df, value_col="Top Speed", title="Top speed by rider", top_n=10)


def render_stats_lab(standings_df: pd.DataFrame) -> None:
    st.subheader("Stats Lab")
    st.caption("Experimental metrics calculated from local DB data.")
    if standings_df.empty:
        st.info("Standings data is required to compute these stats.")
        return

    col1, col2 = st.columns(2)
    with col1:
        render_colored_bar_chart(standings_df, value_col="Power Index", title="Power Index Top 10", top_n=10)
    with col2:
        render_colored_bar_chart(standings_df, value_col="Consistency", title="Consistency Top 10", top_n=10)

    col3, col4 = st.columns(2)
    with col3:
        render_colored_bar_chart(standings_df, value_col="Wins", title="Wins Top 10", top_n=10)
    with col4:
        render_colored_bar_chart(standings_df, value_col="Podiums", title="Podiums Top 10", top_n=10)

    scatter_df = standings_df.copy()
    if not scatter_df.empty:
        scatter_chart = (
            alt.Chart(scatter_df)
            .mark_circle(size=85)
            .encode(
                x=alt.X("Consistency:Q", title="Consistency"),
                y=alt.Y("Points:Q", title="Points"),
                color=alt.Color("BarColor:N", scale=None, legend=None),
                tooltip=[
                    alt.Tooltip("Rider:N"),
                    alt.Tooltip("Points:Q"),
                    alt.Tooltip("Consistency:Q"),
                    alt.Tooltip("Wins:Q"),
                    alt.Tooltip("Podiums:Q"),
                ],
            )
            .properties(title="Points vs Consistency", height=360)
        )
        st.altair_chart(scatter_chart, use_container_width=True)

    st.markdown("**What-if simulator (next weekend)**")
    riders = standings_df["Rider"].tolist()
    rider_pick = st.selectbox("Rider to simulate", riders, key="sim_rider")
    extra_race_points = st.slider("Extra race points", min_value=0, max_value=25, value=10, key="sim_race")
    extra_sprint_points = st.slider("Extra sprint points", min_value=0, max_value=12, value=4, key="sim_sprint")

    projected = standings_df[["Rider", "Points"]].copy()
    projected["Projected Points"] = projected["Points"]
    mask = projected["Rider"] == rider_pick
    projected.loc[mask, "Projected Points"] = (
        projected.loc[mask, "Projected Points"] + extra_race_points + extra_sprint_points
    )
    projected = projected.sort_values("Projected Points", ascending=False).reset_index(drop=True)
    projected["Projected Pos"] = projected.index + 1

    st.dataframe(
        projected[["Projected Pos", "Rider", "Points", "Projected Points"]],
        hide_index=True,
        use_container_width=True,
    )


def main() -> None:
    st.set_page_config(page_title="MotoGP Race Hub", page_icon="🏁", layout="wide")
    st.markdown(
        """
        <style>
            .block-container {padding-top: 1.2rem; padding-bottom: 2rem;}
            .stMetric {border: 1px solid #e8e8e8; border-radius: 10px; padding: 8px 12px;}
            .app-subtitle {color: #4f4f4f; margin-top: -8px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("MotoGP Race Hub")
    st.markdown(
        '<p class="app-subtitle">Results, standings, and advanced stats from your local database.</p>',
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.header("Controls")
        if st.button("Refresh data now"):
            st.cache_data.clear()
            st.rerun()

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
        selected_category = st.selectbox("Category", categories, format_func=lambda row: row.get("name", "Unknown"))
        selected_category_id = str(selected_category["id"])

        st.caption("Data source: local MySQL DB")

    events = get_events_for_year(selected_year)

    tab_overview, tab_standings, tab_results, tab_stats = st.tabs(["Overview", "Standings", "Results", "Stats Lab"])

    with tab_overview:
        render_overview_tab(selected_year, events)

    with tab_standings:
        standings_df = render_standings_tab(selected_year, selected_category_id)

    with tab_results:
        render_results_tab(selected_year, selected_category_id, events)

    with tab_stats:
        render_stats_lab(standings_df if "standings_df" in locals() else pd.DataFrame())


if __name__ == "__main__":
    main()
