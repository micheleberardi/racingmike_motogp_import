import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st

from runtime import get_http_session, request_json

MOTOGP_RESULTS_BASE = "https://api.motogp.pulselive.com/motogp/v1/results"


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


def _parse_iso_date(value: str) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _event_title(event: Dict[str, Any]) -> str:
    return (event.get("sponsored_name") or event.get("name") or "Unknown Event").strip()


def _position_samples(item: Dict[str, Any]) -> List[int]:
    values: List[int] = []
    for key in ("last_positions", "sprint_last_positions"):
        mapping = item.get(key) or {}
        if not isinstance(mapping, dict):
            continue
        for pos in mapping.values():
            number = _safe_int(pos, default=0)
            if number > 0:
                values.append(number)
    return values


def _form_score(item: Dict[str, Any]) -> float:
    positions = _position_samples(item)
    if not positions:
        return 0.0
    weighted = [max(0.0, 30.0 - float(pos * 3)) for pos in positions]
    return round(sum(weighted) / len(weighted), 1)


def _consistency_score(item: Dict[str, Any]) -> float:
    positions = _position_samples(item)
    if not positions:
        return 0.0
    if len(positions) == 1:
        return round(max(0.0, 100.0 - (positions[0] - 1) * 8.0), 1)
    mean = sum(float(x) for x in positions) / len(positions)
    variance = sum((float(x) - mean) ** 2 for x in positions) / len(positions)
    std = math.sqrt(variance)
    return round(max(0.0, 100.0 - std * 18.0), 1)


def _power_index(item: Dict[str, Any]) -> int:
    points = _safe_int(item.get("points"))
    race_wins = _safe_int(item.get("race_wins"))
    podiums = _safe_int(item.get("podiums"))
    sprint_wins = _safe_int(item.get("sprint_wins"))
    sprint_podiums = _safe_int(item.get("sprint_podiums"))
    return points + race_wins * 12 + podiums * 5 + sprint_wins * 4 + sprint_podiums * 2


@st.cache_data(ttl=600, show_spinner=False)
def api_get_json(url: str) -> Any:
    session = get_http_session()
    return request_json(session, url)


@st.cache_data(ttl=3600, show_spinner=False)
def get_seasons() -> List[Dict[str, Any]]:
    return api_get_json(f"{MOTOGP_RESULTS_BASE}/seasons")


@st.cache_data(ttl=3600, show_spinner=False)
def get_categories(season_uuid: str) -> List[Dict[str, Any]]:
    return api_get_json(f"{MOTOGP_RESULTS_BASE}/categories?seasonUuid={season_uuid}")


@st.cache_data(ttl=900, show_spinner=False)
def get_events(season_uuid: str, is_finished: bool) -> List[Dict[str, Any]]:
    raw = api_get_json(f"{MOTOGP_RESULTS_BASE}/events?seasonUuid={season_uuid}&isFinished={str(is_finished).lower()}")
    return [event for event in raw if not event.get("test")]


@st.cache_data(ttl=300, show_spinner=False)
def get_standings(season_uuid: str, category_uuid: str) -> Dict[str, Any]:
    return api_get_json(f"{MOTOGP_RESULTS_BASE}/standings?seasonUuid={season_uuid}&categoryUuid={category_uuid}")


@st.cache_data(ttl=300, show_spinner=False)
def get_sessions(event_uuid: str, category_uuid: str) -> List[Dict[str, Any]]:
    return api_get_json(f"{MOTOGP_RESULTS_BASE}/sessions?eventUuid={event_uuid}&categoryUuid={category_uuid}")


@st.cache_data(ttl=180, show_spinner=False)
def get_session_classification(session_uuid: str) -> Dict[str, Any]:
    return api_get_json(f"{MOTOGP_RESULTS_BASE}/session/{session_uuid}/classification?test=false")


def render_overview_tab(
    year: int,
    finished_events: Sequence[Dict[str, Any]],
    upcoming_events: Sequence[Dict[str, Any]],
) -> None:
    st.subheader("Overview stagione")

    now = datetime.utcnow().date()
    finished_in_year = [e for e in finished_events if (e.get("date_start") or "").startswith(str(year))]
    upcoming_in_year = [e for e in upcoming_events if (e.get("date_start") or "").startswith(str(year))]

    next_round = None
    if upcoming_in_year:
        next_round = sorted(upcoming_in_year, key=lambda e: e.get("date_start") or "")[0]
    last_round = None
    if finished_in_year:
        last_round = sorted(finished_in_year, key=lambda e: e.get("date_end") or "", reverse=True)[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Round chiusi", len(finished_in_year))
    col2.metric("Round in arrivo", len(upcoming_in_year))
    col3.metric("Ultimo round", _event_title(last_round) if last_round else "-")
    col4.metric("Prossimo round", _event_title(next_round) if next_round else "-")

    timeline_rows: List[Dict[str, Any]] = []
    for event in finished_in_year + upcoming_in_year:
        start = _parse_iso_date(f"{event.get('date_start', '')}T00:00:00+00:00")
        end = _parse_iso_date(f"{event.get('date_end', '')}T00:00:00+00:00")
        country = (event.get("country") or {}).get("name", "")
        circuit = (event.get("circuit") or {}).get("name", "")
        status = event.get("status") or ("FINISHED" if event in finished_in_year else "NOT-STARTED")
        timeline_rows.append(
            {
                "Start": start.date().isoformat() if start else "",
                "End": end.date().isoformat() if end else "",
                "Event": _event_title(event),
                "Country": country,
                "Circuit": circuit,
                "Status": status,
            }
        )

    timeline_df = pd.DataFrame(timeline_rows).sort_values("Start")
    st.dataframe(timeline_df, hide_index=True, use_container_width=True)

    if not timeline_df.empty:
        timeline_df["Day"] = pd.to_datetime(timeline_df["Start"], errors="coerce")
        monthly_df = timeline_df.dropna(subset=["Day"]).copy()
        per_month = monthly_df.groupby(monthly_df["Day"].dt.to_period("M")).size()
        per_month.index = per_month.index.astype(str)
        st.caption("Distribuzione round per mese")
        st.bar_chart(per_month)

    st.caption(f"Dati aggiornati rispetto all'API MotoGP in data UTC {now.isoformat()}.")


def build_standings_rows(classification: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in classification:
        rider = item.get("rider") or {}
        team = item.get("team") or {}
        constructor = item.get("constructor") or {}
        rows.append(
            {
                "Pos": _safe_int(item.get("position")),
                "Rider": rider.get("full_name", ""),
                "#": _safe_int(rider.get("number")),
                "Team": team.get("name", ""),
                "Constructor": constructor.get("name", ""),
                "Points": _safe_int(item.get("points")),
                "Race Wins": _safe_int(item.get("race_wins")),
                "Podiums": _safe_int(item.get("podiums")),
                "Sprint Wins": _safe_int(item.get("sprint_wins")),
                "Sprint Podiums": _safe_int(item.get("sprint_podiums")),
                "Power Index": _power_index(item),
                "Form Score": _form_score(item),
                "Consistency": _consistency_score(item),
            }
        )
    return rows


def render_standings_tab(standings_payload: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    st.subheader("Classifica campionato")
    classification = standings_payload.get("classification") or []
    if not classification:
        st.warning("Classifica non disponibile per la selezione corrente.")
        return pd.DataFrame(), []

    rows = build_standings_rows(classification)
    df = pd.DataFrame(rows).sort_values(["Pos", "Points"], ascending=[True, False])

    leader_row = df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Leader", leader_row["Rider"])
    col2.metric("Punti leader", int(leader_row["Points"]))
    col3.metric("Power Index leader", int(leader_row["Power Index"]))
    col4.metric("Rider in classifica", int(len(df)))

    st.dataframe(df, hide_index=True, use_container_width=True)

    top_points = df.sort_values("Points", ascending=False).head(10).set_index("Rider")["Points"]
    st.caption("Top 10 punti")
    st.bar_chart(top_points)

    return df, classification


def render_results_tab(
    category_uuid: str,
    finished_events: Sequence[Dict[str, Any]],
) -> None:
    st.subheader("Risultati sessione")
    if not finished_events:
        st.info("Nessun evento concluso disponibile.")
        return

    ordered_events = sorted(
        [e for e in finished_events if e.get("id")],
        key=lambda e: e.get("date_end") or "",
        reverse=True,
    )
    selected_event = st.selectbox(
        "Seleziona round",
        ordered_events,
        format_func=lambda e: f"{e.get('date_start', '')} - {_event_title(e)}",
    )

    sessions = get_sessions(selected_event["id"], category_uuid)
    if not sessions:
        st.info("Nessuna sessione disponibile per questo round/categoria.")
        return

    sessions_sorted = sorted(
        [s for s in sessions if s.get("id")],
        key=lambda s: s.get("date") or "",
    )
    selected_session = st.selectbox(
        "Seleziona sessione",
        sessions_sorted,
        format_func=lambda s: f"{s.get('date', '')} - {s.get('type', '')}",
    )

    payload = get_session_classification(selected_session["id"])
    rows = []
    for item in payload.get("classification") or []:
        rider = item.get("rider") or {}
        team = item.get("team") or {}
        constructor = item.get("constructor") or {}
        gap = item.get("gap") or {}
        rows.append(
            {
                "Pos": _safe_int(item.get("position")),
                "Rider": rider.get("full_name", ""),
                "#": _safe_int(rider.get("number")),
                "Team": team.get("name", ""),
                "Constructor": constructor.get("name", ""),
                "Laps": _safe_int(item.get("total_laps")),
                "Time": item.get("time", ""),
                "Gap": gap.get("first", ""),
                "Avg Speed": _safe_float(item.get("average_speed")),
                "Top Speed": _safe_float(item.get("top_speed")),
                "Points": _safe_int(item.get("points")),
                "Status": item.get("status", ""),
            }
        )

    results_df = pd.DataFrame(rows).sort_values("Pos")
    if results_df.empty:
        st.warning("Classifica sessione non ancora disponibile.")
        return

    winner = results_df.iloc[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("Winner", winner["Rider"])
    col2.metric("Session", selected_session.get("type", "-"))
    col3.metric("Punti winner", int(winner["Points"]))

    file_url = payload.get("file")
    if file_url:
        st.link_button("Apri PDF ufficiale sessione", file_url)

    st.dataframe(results_df, hide_index=True, use_container_width=True)


def render_stats_lab(df: pd.DataFrame) -> None:
    st.subheader("Stats Lab")
    st.caption("Metriche sperimentali per leggere forma e scenari futuri.")
    if df.empty:
        st.info("Classifica necessaria per calcolare le statistiche.")
        return

    col1, col2 = st.columns(2)
    with col1:
        top_power = df.sort_values("Power Index", ascending=False).head(10).set_index("Rider")["Power Index"]
        st.markdown("**Power Index Top 10**")
        st.bar_chart(top_power)
    with col2:
        top_consistency = df.sort_values("Consistency", ascending=False).head(10).set_index("Rider")["Consistency"]
        st.markdown("**Consistency Top 10**")
        st.bar_chart(top_consistency)

    st.markdown("**What-if simulator (prossimo weekend)**")
    riders = df["Rider"].tolist()
    rider_pick = st.selectbox("Rider da simulare", riders, key="sim_rider")
    extra_race_points = st.slider("Punti gara extra", min_value=0, max_value=25, value=10, key="sim_race")
    extra_sprint_points = st.slider("Punti sprint extra", min_value=0, max_value=12, value=4, key="sim_sprint")

    projected = df[["Rider", "Points"]].copy()
    projected["Projected Points"] = projected["Points"]
    mask = projected["Rider"] == rider_pick
    projected.loc[mask, "Projected Points"] = (
        projected.loc[mask, "Projected Points"] + extra_race_points + extra_sprint_points
    )
    projected = projected.sort_values("Projected Points", ascending=False).reset_index(drop=True)
    projected["Projected Pos"] = projected.index + 1

    st.dataframe(projected[["Projected Pos", "Rider", "Points", "Projected Points"]], hide_index=True, use_container_width=True)


def _build_season_lookup(seasons: Sequence[Dict[str, Any]]) -> Dict[int, str]:
    mapping: Dict[int, str] = {}
    for row in seasons:
        year = row.get("year")
        sid = row.get("id")
        if isinstance(year, int) and sid:
            mapping[year] = sid
    return mapping


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

    st.title("MotoGP Race Hub 2026")
    st.markdown('<p class="app-subtitle">Risultati, classifiche e statistiche avanzate in tempo quasi reale.</p>', unsafe_allow_html=True)

    with st.sidebar:
        st.header("Controlli")
        if st.button("Aggiorna dati ora"):
            st.cache_data.clear()
            st.rerun()

        seasons = get_seasons()
        season_lookup = _build_season_lookup(seasons)
        years = sorted(season_lookup.keys(), reverse=True)
        if not years:
            st.error("Nessuna stagione disponibile dall'API.")
            st.stop()
        default_year = 2026 if 2026 in years else (years[0] if years else datetime.utcnow().year)
        selected_year = st.selectbox("Stagione", years, index=years.index(default_year) if default_year in years else 0)
        season_uuid = season_lookup[selected_year]

        categories = get_categories(season_uuid)
        categories_sorted = sorted(categories, key=lambda c: c.get("legacy_id", 999))
        category = st.selectbox("Categoria", categories_sorted, format_func=lambda c: c.get("name", "Unknown"))
        category_uuid = category["id"]

        st.caption("Fonte dati: MotoGP Public API")

    finished_events = get_events(season_uuid, is_finished=True)
    upcoming_events = get_events(season_uuid, is_finished=False)
    standings_payload = get_standings(season_uuid, category_uuid)

    tab_overview, tab_standings, tab_results, tab_stats = st.tabs(
        ["Overview", "Classifiche", "Risultati", "Stats Lab"]
    )

    with tab_overview:
        render_overview_tab(selected_year, finished_events, upcoming_events)

    with tab_standings:
        standings_df, _ = render_standings_tab(standings_payload)

    with tab_results:
        render_results_tab(category_uuid, finished_events)

    with tab_stats:
        standings_df = standings_df if "standings_df" in locals() else pd.DataFrame()
        render_stats_lab(standings_df)


if __name__ == "__main__":
    main()
