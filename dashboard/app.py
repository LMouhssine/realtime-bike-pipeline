from __future__ import annotations

import html
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import Settings, load_settings


LOGGER = logging.getLogger("citybike.dashboard")
LOCAL_TIMEZONE = "Europe/Paris"
PERCENT_COLUMNS = {
    "utilization_rate",
    "rolling_15m_utilization",
    "avg_utilization_rate",
    "zone_availability_ratio",
    "city_availability_ratio",
}
FLOAT_COLUMNS = {
    "latitude",
    "longitude",
    "avg_bikes_available",
    "avg_free_slots",
    "avg_capacity",
    "imbalance_score",
    "estimated_activity",
}
TIMESTAMP_COLUMNS = {"snapshot_timestamp", "created_at", "ingested_at"}
DISPLAY_LABELS = {
    "city": "Ville",
    "station_name": "Station",
    "bikes_available": "Velos disponibles",
    "free_slots": "Bornes libres",
    "capacity": "Capacite",
    "utilization_rate": "Taux d'utilisation",
    "snapshot_timestamp": "Horodatage",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "rolling_15m_utilization": "Utilisation glissante 15 min",
    "alert_level": "Niveau d'alerte",
    "alert_type": "Type d'alerte",
    "alert_message": "Message d'alerte",
    "created_at": "Cree le",
    "zone_id": "Zone",
    "avg_bikes_available": "Moyenne velos disponibles",
    "avg_free_slots": "Moyenne bornes libres",
    "avg_capacity": "Capacite moyenne",
    "avg_utilization_rate": "Utilisation moyenne",
    "zone_availability_ratio": "Ratio disponibilite zone",
    "city_availability_ratio": "Ratio disponibilite ville",
    "imbalance_score": "Score de desequilibre",
    "usage_date": "Date",
    "usage_hour": "Heure",
    "estimated_activity": "Activite estimee",
    "peak_rank": "Rang du pic",
    "station_count": "Stations",
    "low_stock_stations": "Stations sous tension",
    "shortage_flag": "Zone sous tension",
    "is_empty": "Station vide",
}
ALERT_LEVEL_LABELS = {
    "critical": "critique",
    "empty-and-critical": "vide et critique",
}
ALERT_TYPE_LABELS = {
    "critical": "critique",
    "empty": "station vide",
}
STATION_FOCUS_OPTIONS = (
    "Toutes les stations",
    "Sous tension",
    "Stations vides",
)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=Space+Grotesk:wght@500;700&display=swap');

        :root {
            --ink: #102129;
            --ink-soft: #49606b;
            --teal: #0f766e;
            --teal-soft: rgba(15, 118, 110, 0.12);
            --amber: #d08700;
            --amber-soft: rgba(208, 135, 0, 0.14);
            --coral: #c8553d;
            --coral-soft: rgba(200, 85, 61, 0.14);
            --slate: #58707b;
            --card: rgba(255, 255, 255, 0.82);
            --card-strong: rgba(255, 255, 255, 0.92);
            --border: rgba(16, 33, 41, 0.09);
        }

        html, body, [class*="css"] {
            font-family: "IBM Plex Sans", "Aptos", sans-serif;
            color: var(--ink);
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 36%),
                radial-gradient(circle at bottom right, rgba(208, 135, 0, 0.14), transparent 28%),
                linear-gradient(180deg, #f7f3ea 0%, #eef5f3 48%, #f8f8f6 100%);
        }

        [data-testid="stHeader"] {
            background: rgba(247, 243, 234, 0.75);
            backdrop-filter: blur(12px);
        }

        [data-testid="stSidebar"] {
            background: rgba(255, 255, 255, 0.58);
            backdrop-filter: blur(12px);
        }

        .block-container {
            padding-top: 1.35rem;
            padding-bottom: 2rem;
            max-width: 1420px;
        }

        h1, h2, h3, h4 {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            color: var(--ink) !important;
            letter-spacing: -0.03em;
        }

        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stMarkdownContainer"] strong,
        [data-testid="stMarkdownContainer"] span,
        [data-testid="stMarkdownContainer"] h1,
        [data-testid="stMarkdownContainer"] h2,
        [data-testid="stMarkdownContainer"] h3,
        [data-testid="stMarkdownContainer"] h4,
        [data-testid="stMarkdownContainer"] h5,
        [data-testid="stMarkdownContainer"] h6,
        [data-testid="stCaptionContainer"] p,
        [data-testid="stWidgetLabel"] p {
            color: var(--ink) !important;
        }

        .hero-shell {
            position: relative;
            overflow: hidden;
            padding: 1.6rem 1.7rem 1.2rem 1.7rem;
            border-radius: 30px;
            background:
                linear-gradient(140deg, rgba(255, 255, 255, 0.94), rgba(216, 243, 234, 0.68)),
                linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(255, 255, 255, 0.84));
            border: 1px solid rgba(16, 33, 41, 0.08);
            box-shadow: 0 28px 60px rgba(16, 33, 41, 0.08);
            margin-bottom: 1rem;
        }

        .hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -60px -70px auto;
            width: 220px;
            height: 220px;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(208, 135, 0, 0.18), transparent 62%);
        }

        .hero-kicker {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            padding: 0.45rem 0.8rem;
            border-radius: 999px;
            font-size: 0.82rem;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--teal);
            background: rgba(15, 118, 110, 0.1);
            border: 1px solid rgba(15, 118, 110, 0.12);
        }

        .hero-title {
            margin: 0.85rem 0 0.5rem 0;
            font-size: clamp(2.2rem, 4vw, 3.3rem);
            line-height: 1.02;
            max-width: 12ch;
            color: var(--ink) !important;
        }

        .hero-subtitle {
            max-width: 75ch;
            color: var(--ink-soft) !important;
            font-size: 1rem;
            line-height: 1.65;
            margin-bottom: 1rem;
        }

        .status-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.65rem;
            margin: 0.25rem 0 0.4rem 0;
        }

        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.58rem 0.9rem;
            border-radius: 999px;
            font-size: 0.92rem;
            font-weight: 600;
            border: 1px solid transparent;
            background: rgba(16, 33, 41, 0.07);
            color: var(--ink);
        }

        .status-pill.success {
            background: var(--teal-soft);
            color: var(--teal);
            border-color: rgba(15, 118, 110, 0.16);
        }

        .status-pill.warning {
            background: var(--amber-soft);
            color: var(--amber);
            border-color: rgba(208, 135, 0, 0.16);
        }

        .status-pill.info {
            background: rgba(16, 33, 41, 0.06);
            color: var(--ink);
            border-color: rgba(16, 33, 41, 0.08);
        }

        .toolbar-shell {
            padding: 1rem 1.05rem 0.25rem 1.05rem;
            border-radius: 24px;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(16, 33, 41, 0.08);
            margin: 0.4rem 0 1rem 0;
            box-shadow: 0 16px 34px rgba(16, 33, 41, 0.05);
        }

        .metric-card {
            height: 100%;
            min-height: 152px;
            border-radius: 24px;
            padding: 1rem 1.05rem;
            border: 1px solid var(--border);
            background: var(--card);
            box-shadow: 0 18px 38px rgba(16, 33, 41, 0.05);
        }

        .metric-card__rail {
            width: 44px;
            height: 6px;
            border-radius: 999px;
            margin-bottom: 0.95rem;
            background: linear-gradient(90deg, var(--ink), rgba(16, 33, 41, 0.28));
        }

        .metric-card__rail.teal { background: linear-gradient(90deg, #0f766e, #36b0a6); }
        .metric-card__rail.amber { background: linear-gradient(90deg, #d08700, #f3b34d); }
        .metric-card__rail.coral { background: linear-gradient(90deg, #c8553d, #ea8b67); }
        .metric-card__rail.slate { background: linear-gradient(90deg, #31444c, #7f929a); }

        .metric-card__eyebrow {
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--slate);
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .metric-card__value {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 2.05rem;
            line-height: 1.05;
            margin: 0.5rem 0 0.35rem 0;
            color: var(--ink);
        }

        .metric-card__delta {
            font-size: 0.92rem;
            font-weight: 600;
            margin-bottom: 0.45rem;
        }

        .metric-card__delta.positive { color: var(--teal); }
        .metric-card__delta.negative { color: var(--coral); }
        .metric-card__delta.neutral { color: var(--slate); }

        .metric-card__caption {
            color: var(--slate);
            font-size: 0.92rem;
            line-height: 1.45;
        }

        .section-title {
            font-size: 0.92rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
            color: var(--slate) !important;
            margin: 0.25rem 0 0.75rem 0;
        }

        .city-card {
            height: 100%;
            padding: 1rem;
            border-radius: 22px;
            border: 1px solid var(--border);
            background: var(--card-strong);
            box-shadow: 0 14px 28px rgba(16, 33, 41, 0.05);
        }

        .city-card__name {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 1.08rem;
            margin-bottom: 0.2rem;
            color: var(--ink);
        }

        .city-card__headline {
            font-size: 1.55rem;
            font-weight: 700;
            margin-bottom: 0.4rem;
            color: var(--teal);
        }

        .city-card__meta {
            color: var(--slate);
            font-size: 0.92rem;
            line-height: 1.55;
        }

        .panel-title {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 2rem;
            line-height: 1.08;
            letter-spacing: -0.03em;
            color: var(--ink) !important;
            margin: 0.15rem 0 0.85rem 0;
        }

        .panel-kicker {
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
            color: var(--slate) !important;
            margin: 0.2rem 0 0.45rem 0;
        }

        .muted-note {
            color: var(--ink-soft) !important;
            font-size: 0.95rem;
            line-height: 1.55;
            margin: 0.45rem 0 0.15rem 0;
        }

        .control-label {
            color: var(--ink-soft) !important;
            font-size: 0.88rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.35rem;
        }

        [data-testid="stProgressBar"] > div > div {
            background: linear-gradient(90deg, #0f766e, #36b0a6);
        }

        [data-testid="stTabs"] button[role="tab"] {
            border-radius: 999px;
            padding: 0.55rem 1rem;
            margin-right: 0.35rem;
            border: 1px solid rgba(16, 33, 41, 0.08);
            background: rgba(255, 255, 255, 0.52);
            color: var(--ink);
            font-weight: 600;
        }

        [data-testid="stTabs"] button[aria-selected="true"] {
            background: rgba(15, 118, 110, 0.12);
            border-color: rgba(15, 118, 110, 0.18);
            color: var(--teal);
        }

        [data-testid="stDataFrame"],
        [data-testid="stTable"] {
            background: rgba(255, 255, 255, 0.58);
            border-radius: 24px;
            border: 1px solid rgba(16, 33, 41, 0.06);
        }

        .stButton button {
            border-radius: 16px;
            border: 1px solid rgba(16, 33, 41, 0.08);
            background: linear-gradient(135deg, rgba(16, 33, 41, 0.98), rgba(38, 79, 91, 0.92));
            color: white;
            font-weight: 600;
            box-shadow: 0 10px 18px rgba(16, 33, 41, 0.14);
        }

        .stButton button:hover {
            border-color: rgba(16, 33, 41, 0.08);
            color: white;
        }

        div[data-baseweb="select"] > div {
            background: rgba(255, 255, 255, 0.9);
            color: var(--ink) !important;
        }

        div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="tag"] {
            border-radius: 16px;
        }

        @media (max-width: 900px) {
            .hero-shell {
                padding: 1.2rem 1.1rem 1rem 1.1rem;
            }

            .metric-card {
                min-height: 136px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def get_engine() -> Engine:
    settings = load_settings()
    return create_engine(settings.sqlalchemy_url, pool_pre_ping=True)


def safe_query(query: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    try:
        with get_engine().connect() as connection:
            return pd.read_sql_query(text(query), connection, params=params or {})
    except SQLAlchemyError as exc:
        LOGGER.warning("Dashboard query failed: %s", exc)
        return pd.DataFrame()


def load_overview_metrics() -> dict[str, float | int]:
    metrics = safe_query(
        """
        WITH
        current_metrics AS (
            SELECT
                COUNT(*) AS total_stations,
                COUNT(DISTINCT city) AS tracked_cities,
                COALESCE(AVG(utilization_rate), 0) AS avg_utilization_rate,
                COALESCE(SUM(CASE WHEN bikes_available = 0 THEN 1 ELSE 0 END), 0) AS empty_stations
            FROM latest_station_status
        ),
        critical_metrics AS (
            SELECT COUNT(*) AS critical_station_count
            FROM critical_stations
        ),
        alert_metrics AS (
            SELECT
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') AS alert_count_24h,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 hour') AS alert_count_1h
            FROM station_alerts
        )
        SELECT
            current_metrics.total_stations,
            current_metrics.tracked_cities,
            current_metrics.avg_utilization_rate,
            current_metrics.empty_stations,
            critical_metrics.critical_station_count,
            alert_metrics.alert_count_24h,
            alert_metrics.alert_count_1h,
            0 AS station_delta,
            0.0 AS utilization_delta,
            0 AS empty_station_delta
        FROM current_metrics
        CROSS JOIN critical_metrics
        CROSS JOIN alert_metrics
        """
    )
    if metrics.empty:
        return {
            "total_stations": 0,
            "tracked_cities": 0,
            "avg_utilization_rate": 0.0,
            "empty_stations": 0,
            "critical_station_count": 0,
            "alert_count_24h": 0,
            "alert_count_1h": 0,
            "station_delta": 0,
            "utilization_delta": 0.0,
            "empty_station_delta": 0,
        }

    row = metrics.loc[0]
    return {
        "total_stations": int(row["total_stations"] or 0),
        "tracked_cities": int(row["tracked_cities"] or 0),
        "avg_utilization_rate": float(row["avg_utilization_rate"] or 0.0),
        "empty_stations": int(row["empty_stations"] or 0),
        "critical_station_count": int(row["critical_station_count"] or 0),
        "alert_count_24h": int(row["alert_count_24h"] or 0),
        "alert_count_1h": int(row["alert_count_1h"] or 0),
        "station_delta": int(row["station_delta"] or 0),
        "utilization_delta": float(row["utilization_delta"] or 0.0),
        "empty_station_delta": int(row["empty_station_delta"] or 0),
    }


def load_data_freshness() -> dict[str, Any]:
    freshness = safe_query(
        """
        SELECT
            MAX(snapshot_timestamp) AS latest_snapshot,
            MAX(ingested_at) AS latest_ingested
        FROM latest_station_status
        """
    )
    if freshness.empty or freshness.loc[0, "latest_snapshot"] is None:
        return {"latest_snapshot": None, "latest_ingested": None}

    return {
        "latest_snapshot": pd.Timestamp(freshness.loc[0, "latest_snapshot"]),
        "latest_ingested": pd.Timestamp(freshness.loc[0, "latest_ingested"]),
    }


def load_latest_station_status() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            station_name,
            latitude,
            longitude,
            bikes_available,
            free_slots,
            capacity,
            utilization_rate,
            zone_id,
            snapshot_timestamp,
            ingested_at
        FROM latest_station_status
        ORDER BY city ASC, station_name ASC
        """
    )


def load_top_utilization_stations(limit: int) -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            station_name,
            bikes_available,
            free_slots,
            capacity,
            utilization_rate,
            zone_id,
            snapshot_timestamp,
            utilization_rank
        FROM top_utilization_stations
        ORDER BY utilization_rank ASC, station_name ASC
        LIMIT :limit
        """,
        {"limit": limit},
    )


def load_city_summary() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            COUNT(*) AS station_count,
            COALESCE(AVG(utilization_rate), 0) AS avg_utilization_rate,
            COALESCE(AVG(bikes_available), 0) AS avg_bikes_available,
            COALESCE(SUM(CASE WHEN bikes_available = 0 THEN 1 ELSE 0 END), 0) AS empty_stations,
            COALESCE(SUM(CASE WHEN bikes_available <= 2 THEN 1 ELSE 0 END), 0) AS low_stock_stations
        FROM latest_station_status
        GROUP BY city
        ORDER BY avg_utilization_rate DESC, city ASC
        """
    )


def load_low_availability_zones() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            zone_id,
            station_count,
            avg_bikes_available,
            avg_free_slots,
            avg_utilization_rate,
            shortage_flag,
            snapshot_timestamp
        FROM low_availability_zones
        ORDER BY shortage_flag DESC, avg_bikes_available ASC, city ASC
        """
    )


def load_daily_usage_peaks() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            usage_date,
            usage_hour,
            estimated_activity,
            peak_rank
        FROM daily_usage_peaks
        ORDER BY usage_date DESC, city ASC, usage_hour ASC
        """
    )


def load_critical_stations() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            station_name,
            bikes_available,
            free_slots,
            utilization_rate,
            rolling_15m_utilization,
            is_empty,
            snapshot_timestamp,
            alert_level
        FROM critical_stations
        ORDER BY snapshot_timestamp DESC, city ASC, station_name ASC
        """
    )


def load_station_alerts() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            station_name,
            alert_type,
            alert_message,
            bikes_available,
            free_slots,
            utilization_rate,
            snapshot_timestamp,
            created_at
        FROM station_alerts
        ORDER BY created_at DESC
        LIMIT 100
        """
    )


def load_geographic_imbalance() -> pd.DataFrame:
    return safe_query(
        """
        SELECT
            city,
            zone_id,
            avg_bikes_available,
            avg_capacity,
            zone_availability_ratio,
            city_availability_ratio,
            imbalance_score,
            snapshot_timestamp
        FROM geographic_imbalance
        ORDER BY ABS(imbalance_score) DESC, city ASC
        """
    )


def filter_by_cities(df: pd.DataFrame, selected_cities: list[str]) -> pd.DataFrame:
    if df.empty or "city" not in df.columns or not selected_cities:
        return df.copy()
    return df[df["city"].isin(selected_cities)].copy()


def apply_station_focus(df: pd.DataFrame, station_focus: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if station_focus == "Sous tension":
        return df[
            (df["utilization_rate"] >= 0.85) | (df["bikes_available"] <= 2)
        ].copy()
    if station_focus == "Stations vides":
        return df[df["bikes_available"] <= 0].copy()
    return df.copy()


def prepare_map_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame["lat"] = frame["latitude"]
    frame["lon"] = frame["longitude"]
    return frame.dropna(subset=["lat", "lon"])


def to_local_timestamp(value: pd.Timestamp | Any) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert(LOCAL_TIMEZONE)


def format_duration(total_seconds: int) -> str:
    hours, remainder = divmod(max(total_seconds, 0), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours} h {minutes:02d} min"
    if minutes:
        return f"{minutes} min {seconds:02d} s"
    return f"{seconds} s"


def format_timestamp_display(value: pd.Timestamp | Any) -> str:
    timestamp = to_local_timestamp(value)
    if timestamp is None:
        return "-"
    return timestamp.strftime("%d/%m/%Y %H:%M:%S")


def render_panel_title(title: str, *, kicker: str | None = None) -> None:
    parts: list[str] = []
    if kicker:
        parts.append(f'<div class="panel-kicker">{html.escape(kicker)}</div>')
    parts.append(f'<div class="panel-title">{html.escape(title)}</div>')
    st.markdown("".join(parts), unsafe_allow_html=True)


def render_muted_note(text: str) -> None:
    st.markdown(f'<p class="muted-note">{html.escape(text)}</p>', unsafe_allow_html=True)


def build_live_monitor_state(
    freshness: dict[str, Any],
    poll_interval_seconds: int,
    *,
    now: pd.Timestamp | None = None,
) -> dict[str, Any]:
    latest_snapshot = freshness.get("latest_snapshot")
    latest_ingested = freshness.get("latest_ingested")
    level, message = describe_freshness(
        latest_snapshot,
        poll_interval_seconds,
        now=now,
    )
    if latest_snapshot is None:
        return {
            "level": level,
            "message": message,
            "progress": 0.0,
            "progress_text": "En attente du premier snapshot traite.",
            "latest_snapshot_label": "-",
            "latest_ingested_label": "-",
        }

    reference_now = now or pd.Timestamp.now(tz="UTC")
    snapshot_utc = (
        latest_snapshot.tz_localize("UTC")
        if latest_snapshot.tzinfo is None
        else latest_snapshot.tz_convert("UTC")
    )
    age_seconds = max(int((reference_now - snapshot_utc).total_seconds()), 0)
    cycle_progress = min(age_seconds / max(poll_interval_seconds, 1), 1.0)

    if age_seconds <= poll_interval_seconds:
        progress_text = (
            "Prochaine mise a jour theorique dans "
            f"{format_duration(poll_interval_seconds - age_seconds)}."
        )
    else:
        progress_text = (
            "Flux en retard de "
            f"{format_duration(age_seconds - poll_interval_seconds)} par rapport au rythme attendu."
        )

    return {
        "level": level,
        "message": message,
        "progress": cycle_progress,
        "progress_text": progress_text,
        "latest_snapshot_label": format_timestamp_display(latest_snapshot),
        "latest_ingested_label": format_timestamp_display(latest_ingested),
    }


def localize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    localized = df.copy()

    if "alert_level" in localized.columns:
        localized["alert_level"] = localized["alert_level"].replace(ALERT_LEVEL_LABELS)
    if "alert_type" in localized.columns:
        localized["alert_type"] = localized["alert_type"].replace(ALERT_TYPE_LABELS)
    if "shortage_flag" in localized.columns:
        localized["shortage_flag"] = localized["shortage_flag"].map(
            lambda value: "oui" if bool(value) else "non"
        )
    if "is_empty" in localized.columns:
        localized["is_empty"] = localized["is_empty"].map(
            lambda value: "oui" if bool(value) else "non"
        )

    for column in TIMESTAMP_COLUMNS.intersection(localized.columns):
        localized[column] = localized[column].map(format_timestamp_display)

    for column in PERCENT_COLUMNS.intersection(localized.columns):
        localized[column] = localized[column].map(
            lambda value: f"{float(value):.1%}" if pd.notna(value) else "-"
        )

    for column in FLOAT_COLUMNS.intersection(localized.columns):
        localized[column] = localized[column].map(
            lambda value: round(float(value), 2) if pd.notna(value) else None
        )

    return localized.rename(columns=DISPLAY_LABELS)


def format_count(value: int | float) -> str:
    return f"{int(value):,}".replace(",", " ")


def render_metric_card(
    container: Any,
    *,
    eyebrow: str,
    value: str,
    delta_text: str,
    caption: str,
    accent: str,
) -> None:
    delta_class = "neutral"
    if delta_text.startswith("+"):
        delta_class = "positive"
    elif delta_text.startswith("-"):
        delta_class = "negative"

    container.markdown(
        f"""
        <section class="metric-card">
            <div class="metric-card__rail {html.escape(accent)}"></div>
            <div class="metric-card__eyebrow">{html.escape(eyebrow)}</div>
            <div class="metric-card__value">{html.escape(value)}</div>
            <div class="metric-card__delta {delta_class}">{html.escape(delta_text)}</div>
            <div class="metric-card__caption">{html.escape(caption)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(
    settings: Settings,
    metrics: dict[str, float | int],
    freshness: dict[str, Any],
    city_options: list[str],
    selected_cities: list[str],
) -> None:
    live_state = build_live_monitor_state(
        freshness,
        settings.poll_interval_seconds,
    )
    selected_scope = (
        f"{len(selected_cities)} / {len(city_options)} villes visibles"
        if city_options
        else "Aucune ville active"
    )
    st.markdown(
        f"""
        <section class="hero-shell">
            <div class="hero-kicker">flux terrain France</div>
            <h1 class="hero-title">Pilotage live du reseau CityBikes</h1>
            <p class="hero-subtitle">
                Les KPI ci-dessous representent le cockpit national. Les vues d'exploration se
                rechargent automatiquement toutes les {settings.dashboard_refresh_seconds} secondes
                pour suivre Kafka, Spark et PostgreSQL sans rafraichissement manuel.
            </p>
            <div class="status-row">
                <span class="status-pill {html.escape(live_state['level'])}">
                    {html.escape(live_state['message'])}
                </span>
                <span class="status-pill info">
                    Dernier snapshot: {html.escape(live_state['latest_snapshot_label'])}
                </span>
                <span class="status-pill info">
                    Derniere ingestion: {html.escape(live_state['latest_ingested_label'])}
                </span>
                <span class="status-pill info">
                    Fenetre exploree: {html.escape(selected_scope)}
                </span>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.progress(live_state["progress"])
    render_muted_note(live_state["progress_text"])
    render_muted_note(
        "Cadence producteur attendue: "
        f"{settings.poll_interval_seconds} s. Stations suivies au niveau national: {metrics['total_stations']}."
    )


def render_metric_grid(metrics: dict[str, float | int]) -> None:
    first_row = st.columns(3)
    second_row = st.columns(3)
    render_metric_card(
        first_row[0],
        eyebrow="Stations suivies",
        value=format_count(metrics["total_stations"]),
        delta_text="Snapshot consolide courant",
        caption="Volume courant present dans le dernier snapshot consolide.",
        accent="teal",
    )
    render_metric_card(
        first_row[1],
        eyebrow="Villes actives",
        value=format_count(metrics["tracked_cities"]),
        delta_text="Couverture stable",
        caption="Nombre de villes francaises actuellement visibles dans la couche or.",
        accent="slate",
    )
    render_metric_card(
        first_row[2],
        eyebrow="Utilisation moyenne",
        value=f"{float(metrics['avg_utilization_rate']):.1%}",
        delta_text="Recalculee automatiquement",
        caption="Charge moyenne du reseau sur le dernier etat disponible.",
        accent="amber",
    )
    render_metric_card(
        second_row[0],
        eyebrow="Stations vides",
        value=format_count(metrics["empty_stations"]),
        delta_text="Lecture live du dernier snapshot",
        caption="Nombre de stations sans velo dans l'etat courant consolide.",
        accent="coral",
    )
    render_metric_card(
        second_row[1],
        eyebrow="Stations critiques",
        value=format_count(metrics["critical_station_count"]),
        delta_text="Lecture live de la table critique",
        caption="Stations vides ou sous forte tension selon les regles metier.",
        accent="slate",
    )
    render_metric_card(
        second_row[2],
        eyebrow="Alertes 24 h",
        value=format_count(metrics["alert_count_24h"]),
        delta_text=f"{int(metrics['alert_count_1h'])} nouvelle(s) sur 1 h",
        caption="Alertes recemment inserees dans PostgreSQL pour vide ou criticite.",
        accent="teal",
    )


def render_city_pulse(city_summary: pd.DataFrame) -> None:
    if city_summary.empty:
        st.info("Le radar par ville apparaitra des que le premier micro-batch sera disponible.")
        return

    st.markdown(
        '<div class="section-title">Radar terrain par ville</div>',
        unsafe_allow_html=True,
    )

    rows = list(city_summary.itertuples(index=False))
    cards_per_row = 3

    for start in range(0, len(rows), cards_per_row):
        row_slice = rows[start : start + cards_per_row]
        columns = st.columns(len(row_slice))
        for column, row in zip(columns, row_slice):
            column.markdown(
                (
                    '<article class="city-card">'
                    f'<div class="city-card__name">{html.escape(str(row.city))}</div>'
                    f'<div class="city-card__headline">{float(row.avg_utilization_rate):.1%}</div>'
                    '<div class="city-card__meta">'
                    f'{int(row.station_count)} stations<br/>'
                    f'{float(row.avg_bikes_available):.1f} velos en moyenne<br/>'
                    f'{int(row.empty_stations)} station(s) vide(s)<br/>'
                    f'{int(row.low_stock_stations)} station(s) sous tension'
                    "</div>"
                    "</article>"
                ),
                unsafe_allow_html=True,
            )


def build_station_map_frame(df: pd.DataFrame) -> pd.DataFrame:
    map_frame = prepare_map_frame(df)
    if map_frame.empty:
        return map_frame

    map_frame = map_frame.copy()
    map_frame["utilization_label"] = map_frame["utilization_rate"].map(lambda value: f"{value:.1%}")
    map_frame["snapshot_label"] = map_frame["snapshot_timestamp"].map(format_timestamp_display)
    map_frame["radius"] = map_frame["capacity"].clip(lower=5).fillna(5) * 18

    colors: list[list[int]] = []
    for row in map_frame.itertuples(index=False):
        if row.bikes_available <= 0:
            colors.append([200, 85, 61, 220])
        elif row.utilization_rate >= 0.85:
            colors.append([208, 135, 0, 210])
        else:
            colors.append([15, 118, 110, 190])
    map_frame["fill_color"] = colors
    return map_frame


def render_overview(
    city_summary: pd.DataFrame,
    top_stations: pd.DataFrame,
    low_availability: pd.DataFrame,
) -> None:
    render_city_pulse(city_summary)

    left, right = st.columns([1.35, 1.0])

    with left:
        render_panel_title("Stations les plus sollicitees")
        if top_stations.empty:
            st.info("Aucune station a fort taux d'utilisation n'est encore disponible.")
        else:
            st.dataframe(
                localize_frame(top_stations),
                use_container_width=True,
                hide_index=True,
            )

    with right:
        render_panel_title("Zones en tension")
        if low_availability.empty:
            st.info("Aucune zone en tension n'est actuellement remontee.")
        else:
            focus_frame = low_availability.copy()
            if "shortage_flag" in focus_frame.columns:
                focus_frame = focus_frame[focus_frame["shortage_flag"]]
            st.dataframe(
                localize_frame(focus_frame.head(12)),
                use_container_width=True,
                hide_index=True,
            )


def render_station_map(latest_status: pd.DataFrame, station_focus: str) -> None:
    map_frame = build_station_map_frame(latest_status)
    if map_frame.empty:
        st.info("La carte live apparaitra apres le premier micro-batch traite avec succes.")
        return

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_frame,
        get_position="[lon, lat]",
        get_fill_color="fill_color",
        get_radius="radius",
        pickable=True,
        auto_highlight=True,
        stroked=True,
        get_line_color=[16, 33, 41, 85],
        line_width_min_pixels=1,
    )
    view_state = pdk.ViewState(
        latitude=float(map_frame["lat"].mean()),
        longitude=float(map_frame["lon"].mean()),
        zoom=5.2,
        pitch=28,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[scatter_layer],
            initial_view_state=view_state,
            tooltip={
                "html": (
                    "<b>{station_name}</b><br/>"
                    "{city}<br/>"
                    "Velos: {bikes_available} | Bornes: {free_slots}<br/>"
                    "Utilisation: {utilization_label}<br/>"
                    "Snapshot: {snapshot_label}"
                )
            },
        ),
        use_container_width=True,
    )
    st.caption(
        f"{len(map_frame)} station(s) visibles sur la carte avec le filtre '{station_focus.lower()}'."
    )
    with st.expander("Voir le detail des stations visibles", expanded=False):
        st.dataframe(
            localize_frame(
                map_frame[
                    [
                        "city",
                        "station_name",
                        "bikes_available",
                        "free_slots",
                        "capacity",
                        "utilization_rate",
                        "snapshot_timestamp",
                    ]
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_utilization_heatmap(latest_status: pd.DataFrame) -> None:
    heatmap_df = prepare_map_frame(latest_status)
    if heatmap_df.empty:
        st.info("La carte de chaleur attend l'arrivee de donnees en direct.")
        return

    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=heatmap_df,
        get_position="[lon, lat]",
        get_weight="utilization_rate",
        radiusPixels=55,
        intensity=1.2,
        threshold=0.12,
    )
    view_state = pdk.ViewState(
        latitude=float(heatmap_df["lat"].mean()),
        longitude=float(heatmap_df["lon"].mean()),
        zoom=5.1,
        pitch=34,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[heatmap_layer],
            initial_view_state=view_state,
        ),
        use_container_width=True,
    )
    st.caption(
        "Les zones denses representent les poches de forte utilisation a partir du dernier snapshot consolide."
    )


def render_time_series(peaks: pd.DataFrame) -> None:
    if peaks.empty:
        st.info(
            "L'analyse des pics d'usage se remplira une fois qu'un historique suffisant de snapshots sera disponible."
        )
        return

    render_panel_title("Pics d'usage", kicker="Historique inferre")
    chart_frame = peaks.copy()
    chart_frame["label"] = (
        chart_frame["usage_date"].astype(str)
        + " "
        + chart_frame["usage_hour"].astype(str).str.zfill(2)
        + ":00"
    )
    chart_data = (
        chart_frame.pivot_table(
            index="label",
            columns="city",
            values="estimated_activity",
            aggfunc="sum",
        )
        .fillna(0.0)
        .sort_index()
    )
    st.line_chart(chart_data, use_container_width=True)

    spotlight = chart_frame.sort_values(
        ["peak_rank", "estimated_activity"],
        ascending=[True, False],
    ).head(15)
    st.dataframe(
        localize_frame(spotlight.drop(columns=["label"])),
        use_container_width=True,
        hide_index=True,
    )


def render_alerts(critical: pd.DataFrame, alerts: pd.DataFrame) -> None:
    if critical.empty and alerts.empty:
        st.info("Aucune alerte de station vide ou critique n'a encore ete generee.")
        return

    summary_columns = st.columns(3)
    critical_count = 0 if critical.empty else len(critical)
    empty_alerts = 0 if alerts.empty else int((alerts["alert_type"] == "empty").sum())
    critical_alerts = 0 if alerts.empty else int((alerts["alert_type"] == "critical").sum())

    render_metric_card(
        summary_columns[0],
        eyebrow="Stations critiques",
        value=format_count(critical_count),
        delta_text="Sur le dernier etat",
        caption="Stations actuellement marquees comme critiques dans la table or.",
        accent="coral",
    )
    render_metric_card(
        summary_columns[1],
        eyebrow="Alertes vides",
        value=format_count(empty_alerts),
        delta_text="Fenetre des 100 dernieres alertes",
        caption="Stations devenues vides et historisees dans PostgreSQL.",
        accent="amber",
    )
    render_metric_card(
        summary_columns[2],
        eyebrow="Alertes critiques",
        value=format_count(critical_alerts),
        delta_text="Fenetre des 100 dernieres alertes",
        caption="Alertes de criticite dues a la combinaison stock faible + forte demande.",
        accent="teal",
    )

    left, right = st.columns([1.0, 1.25])
    with left:
        render_panel_title("Stations critiques")
        if critical.empty:
            st.info("Aucune station critique active dans le dernier snapshot.")
        else:
            st.dataframe(
                localize_frame(critical),
                use_container_width=True,
                hide_index=True,
            )

    with right:
        render_panel_title("Alertes recentes")
        if alerts.empty:
            st.info("Aucune alerte recente n'a encore ete capturee.")
        else:
            st.dataframe(
                localize_frame(alerts),
                use_container_width=True,
                hide_index=True,
            )


def render_geographic_imbalance(
    imbalance: pd.DataFrame,
    low_availability: pd.DataFrame,
) -> None:
    if imbalance.empty and low_availability.empty:
        st.info("L'analyse du desequilibre geographique apparaitra apres le premier calcul analytique.")
        return

    render_panel_title("Desequilibre geographique", kicker="Lecture spatiale")
    if not imbalance.empty:
        chart_source = imbalance.copy().head(12)
        chart_source["zone_label"] = chart_source["city"] + " | " + chart_source["zone_id"]
        st.bar_chart(
            chart_source.set_index("zone_label")["imbalance_score"],
            use_container_width=True,
        )

    left, right = st.columns([1.2, 1.0])
    with left:
        render_panel_title("Zones les plus desequilibrees")
        if imbalance.empty:
            st.info("Aucun score de desequilibre n'est encore disponible.")
        else:
            st.dataframe(
                localize_frame(imbalance.head(15)),
                use_container_width=True,
                hide_index=True,
            )
    with right:
        render_panel_title("Zones a faible disponibilite")
        if low_availability.empty:
            st.info("Aucune zone de faible disponibilite n'est encore disponible.")
        else:
            focus_frame = low_availability.copy()
            if "shortage_flag" in focus_frame.columns:
                focus_frame = focus_frame[focus_frame["shortage_flag"]]
            st.dataframe(
                localize_frame(focus_frame.head(15)),
                use_container_width=True,
                hide_index=True,
            )


def describe_freshness(
    latest_snapshot: pd.Timestamp | None,
    poll_interval_seconds: int,
    *,
    now: pd.Timestamp | None = None,
) -> tuple[str, str]:
    if latest_snapshot is None:
        return (
            "info",
            "Aucun snapshot n'est encore disponible dans PostgreSQL.",
        )

    reference_now = now or pd.Timestamp.now(tz="UTC")
    snapshot_ts = (
        latest_snapshot.tz_localize("UTC")
        if latest_snapshot.tzinfo is None
        else latest_snapshot.tz_convert("UTC")
    )
    age_seconds = max(int((reference_now - snapshot_ts).total_seconds()), 0)
    age_minutes, remaining_seconds = divmod(age_seconds, 60)

    if age_seconds <= poll_interval_seconds * 2:
        return (
            "success",
            f"Dernier snapshot disponible il y a {age_minutes} min {remaining_seconds:02d} s.",
        )

    return (
        "warning",
        "Les donnees sont anciennes. Le producteur ou l'API CityBikes est probablement en pause ou limitee "
        f"(dernier snapshot il y a {age_minutes} min {remaining_seconds:02d} s).",
    )


def render_dashboard(settings: Settings) -> None:
    metrics = load_overview_metrics()
    latest_status = load_latest_station_status()
    top_stations = load_top_utilization_stations(settings.top_station_limit)
    city_summary = load_city_summary()
    low_availability = load_low_availability_zones()
    peaks = load_daily_usage_peaks()
    critical = load_critical_stations()
    alerts = load_station_alerts()
    imbalance = load_geographic_imbalance()
    freshness = load_data_freshness()

    city_options = sorted(latest_status["city"].dropna().unique().tolist())
    if not city_options:
        city_options = list(settings.target_cities)

    render_page_header(
        settings,
        metrics,
        freshness,
        city_options,
        st.session_state.get("city_filter", city_options),
    )
    render_metric_grid(metrics)

    st.markdown(
        '<div class="section-title">Filtres d\'exploration</div>',
        unsafe_allow_html=True,
    )
    control_left, control_center, control_right = st.columns([2.3, 1.5, 0.9])
    control_left.markdown(
        '<div class="control-label">Villes explorees</div>',
        unsafe_allow_html=True,
    )
    selected_cities = control_left.multiselect(
        "Villes explorees",
        options=city_options,
        default=city_options,
        key="city_filter",
        label_visibility="collapsed",
        help="Ces filtres pilotent les cartes et tableaux d'exploration sans changer le cockpit national.",
    )
    control_center.markdown(
        '<div class="control-label">Lecture carte</div>',
        unsafe_allow_html=True,
    )
    station_focus = control_center.selectbox(
        "Lecture carte",
        options=STATION_FOCUS_OPTIONS,
        index=0,
        label_visibility="collapsed",
        help="Permet de focaliser rapidement la carte sur les stations sous tension ou vides.",
    )
    control_right.markdown(
        '<div class="control-label">Action</div>',
        unsafe_allow_html=True,
    )
    if control_right.button("Actualiser", use_container_width=True):
        st.rerun()

    filtered_city_summary = filter_by_cities(city_summary, selected_cities)
    filtered_top_stations = filter_by_cities(top_stations, selected_cities)
    filtered_low_availability = filter_by_cities(low_availability, selected_cities)
    filtered_peaks = filter_by_cities(peaks, selected_cities)
    filtered_critical = filter_by_cities(critical, selected_cities)
    filtered_alerts = filter_by_cities(alerts, selected_cities)
    filtered_imbalance = filter_by_cities(imbalance, selected_cities)
    filtered_latest_status = filter_by_cities(latest_status, selected_cities)
    focused_latest_status = apply_station_focus(filtered_latest_status, station_focus)

    tabs = st.tabs(
        [
            "Cockpit",
            "Carte live",
            "Heatmap",
            "Pics d'usage",
            "Desequilibre",
            "Alertes",
        ]
    )

    with tabs[0]:
        render_overview(
            filtered_city_summary,
            filtered_top_stations,
            filtered_low_availability,
        )
    with tabs[1]:
        render_station_map(focused_latest_status, station_focus)
    with tabs[2]:
        render_utilization_heatmap(filtered_latest_status)
    with tabs[3]:
        render_time_series(filtered_peaks)
    with tabs[4]:
        render_geographic_imbalance(filtered_imbalance, filtered_low_availability)
    with tabs[5]:
        render_alerts(filtered_critical, filtered_alerts)


def main() -> None:
    configure_logging()
    settings = load_settings()

    st.set_page_config(
        page_title="Analyse CityBike en Temps Reel",
        page_icon=":bike:",
        layout="wide",
    )
    apply_theme()

    @st.fragment(run_every=settings.dashboard_refresh_seconds)
    def live_dashboard() -> None:
        render_dashboard(settings)

    live_dashboard()


if __name__ == "__main__":
    main()
