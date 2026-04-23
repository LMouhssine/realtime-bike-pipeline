from __future__ import annotations

import html
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import altair as alt
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
MAP_MODE_OPTIONS = ("Stations", "Heatmap")
STATION_FOCUS_OPTIONS = (
    "Toutes les stations",
    "Sous tension",
    "Stations vides",
)
CITY_FILTER_WIDGET_KEY = "dashboard_selected_cities_widget"
DELTA_KEYS = (
    "total_stations",
    "avg_utilization_rate",
    "empty_stations",
    "critical_station_count",
)
PERCENT_COLUMNS = {
    "utilization_rate",
    "rolling_15m_utilization",
    "avg_utilization_rate",
    "avg_utilization",
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
LATEST_STATUS_COLUMNS = (
    "city",
    "station_name",
    "latitude",
    "longitude",
    "bikes_available",
    "free_slots",
    "capacity",
    "utilization_rate",
    "zone_id",
    "snapshot_timestamp",
    "ingested_at",
)
TOP_STATIONS_COLUMNS = (
    "city",
    "station_name",
    "bikes_available",
    "free_slots",
    "capacity",
    "utilization_rate",
    "zone_id",
    "snapshot_timestamp",
    "utilization_rank",
)
CITY_SUMMARY_COLUMNS = (
    "city",
    "station_count",
    "avg_utilization_rate",
    "avg_bikes_available",
    "empty_stations",
    "low_stock_stations",
)
LOW_AVAILABILITY_COLUMNS = (
    "city",
    "zone_id",
    "station_count",
    "avg_bikes_available",
    "avg_free_slots",
    "avg_utilization_rate",
    "shortage_flag",
    "snapshot_timestamp",
)
PEAKS_COLUMNS = (
    "city",
    "usage_date",
    "usage_hour",
    "estimated_activity",
    "peak_rank",
)
CRITICAL_COLUMNS = (
    "city",
    "station_name",
    "bikes_available",
    "free_slots",
    "utilization_rate",
    "rolling_15m_utilization",
    "is_empty",
    "snapshot_timestamp",
    "alert_level",
)
ALERTS_COLUMNS = (
    "city",
    "station_name",
    "alert_type",
    "alert_message",
    "bikes_available",
    "free_slots",
    "utilization_rate",
    "snapshot_timestamp",
    "created_at",
)
IMBALANCE_COLUMNS = (
    "city",
    "zone_id",
    "avg_bikes_available",
    "avg_capacity",
    "zone_availability_ratio",
    "city_availability_ratio",
    "imbalance_score",
    "snapshot_timestamp",
)
DISPLAY_LABELS = {
    "city": "Ville",
    "station_name": "Station",
    "bikes_available": "Velos disponibles",
    "free_slots": "Bornes libres",
    "capacity": "Capacite",
    "utilization_rate": "Taux d'utilisation",
    "rolling_15m_utilization": "Utilisation glissante 15 min",
    "snapshot_timestamp": "Horodatage",
    "created_at": "Cree le",
    "ingested_at": "Ingere le",
    "latitude": "Latitude",
    "longitude": "Longitude",
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
    "alert_level": "Niveau d'alerte",
    "alert_type": "Type d'alerte",
    "alert_message": "Message d'alerte",
    "utilization_rank": "Rang",
}
ALERT_LEVEL_LABELS = {
    "critical": "critique",
    "empty-and-critical": "vide et critique",
}
ALERT_TYPE_LABELS = {
    "critical": "critique",
    "empty": "station vide",
}


@dataclass(frozen=True)
class DashboardPayload:
    metrics: dict[str, float | int]
    freshness: dict[str, Any]
    latest_status: pd.DataFrame
    top_stations: pd.DataFrame
    city_summary: pd.DataFrame
    low_availability: pd.DataFrame
    peaks: pd.DataFrame
    critical: pd.DataFrame
    alerts: pd.DataFrame
    imbalance: pd.DataFrame


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');

        :root {
            --bg: #f6f8f7;
            --surface: #ffffff;
            --surface-muted: #f1f4f3;
            --border: #dfe6e3;
            --ink: #11232b;
            --ink-soft: #5f747c;
            --teal: #0f766e;
            --teal-soft: #ddf4f1;
            --amber: #b7791f;
            --amber-soft: #fff3df;
            --coral: #c8553d;
            --coral-soft: #feebe7;
            --shadow: 0 12px 28px rgba(17, 35, 43, 0.06);
            --radius-lg: 24px;
            --radius-md: 18px;
            --radius-sm: 14px;
        }

        html, body, [class*="css"] {
            font-family: "Manrope", "Segoe UI Variable", "Segoe UI", sans-serif;
            color: var(--ink);
        }

        [data-testid="stAppViewContainer"] {
            background: var(--bg);
        }

        [data-testid="stHeader"] {
            background: rgba(246, 248, 247, 0.96);
            border-bottom: 1px solid rgba(17, 35, 43, 0.04);
        }

        [data-testid="stSidebar"] {
            background: var(--surface);
        }

        .block-container {
            max-width: 1380px;
            padding-top: 1.2rem;
            padding-bottom: 2rem;
        }

        [data-testid="stMarkdownContainer"] p,
        [data-testid="stMarkdownContainer"] li,
        [data-testid="stMarkdownContainer"] span,
        [data-testid="stMarkdownContainer"] strong,
        [data-testid="stWidgetLabel"] p {
            color: var(--ink);
        }

        .topbar {
            padding: 1.1rem 1.15rem;
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            background: var(--surface);
            box-shadow: var(--shadow);
            margin-bottom: 1rem;
        }

        .topbar__kicker {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            padding: 0.4rem 0.7rem;
            border-radius: 999px;
            background: var(--surface-muted);
            color: var(--teal);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .topbar__title {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: clamp(2rem, 3.2vw, 3rem);
            line-height: 1.05;
            letter-spacing: -0.04em;
            margin: 0.8rem 0 0.35rem 0;
            color: var(--ink);
        }

        .topbar__subtitle {
            margin: 0;
            max-width: 68ch;
            color: var(--ink-soft);
            font-size: 0.98rem;
            line-height: 1.55;
        }

        .status-strip {
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
            margin-top: 0.95rem;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            min-height: 42px;
            padding: 0.55rem 0.85rem;
            border-radius: 999px;
            border: 1px solid var(--border);
            background: var(--surface-muted);
            color: var(--ink);
            font-size: 0.9rem;
            font-weight: 600;
            line-height: 1.3;
        }

        .status-badge.live {
            border-color: #bce3dc;
            background: var(--teal-soft);
            color: var(--teal);
        }

        .status-badge.warning {
            border-color: #f3d5a3;
            background: var(--amber-soft);
            color: var(--amber);
        }

        .status-badge.info {
            background: var(--surface-muted);
            color: var(--ink);
        }

        .section-label {
            margin: 0.2rem 0 0.65rem 0;
            color: var(--ink-soft);
            font-size: 0.82rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .metric-card {
            height: 100%;
            min-height: 156px;
            padding: 1rem 1rem 0.95rem 1rem;
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--shadow);
        }

        .metric-card__bar {
            width: 42px;
            height: 6px;
            border-radius: 999px;
            margin-bottom: 0.8rem;
            background: linear-gradient(90deg, var(--ink), rgba(17, 35, 43, 0.25));
        }

        .metric-card__bar.teal { background: linear-gradient(90deg, #0f766e, #42a89a); }
        .metric-card__bar.amber { background: linear-gradient(90deg, #b7791f, #dca455); }
        .metric-card__bar.coral { background: linear-gradient(90deg, #c8553d, #e38c78); }
        .metric-card__bar.slate { background: linear-gradient(90deg, #334c55, #7c8e95); }

        .metric-card__eyebrow {
            color: var(--ink-soft);
            font-size: 0.8rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }

        .metric-card__value {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 2.1rem;
            line-height: 1.05;
            margin: 0.45rem 0 0.3rem 0;
            color: var(--ink);
        }

        .metric-card__trend {
            display: inline-flex;
            align-items: center;
            min-height: 30px;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            font-size: 0.82rem;
            font-weight: 700;
            background: var(--surface-muted);
            color: var(--ink-soft);
        }

        .metric-card__trend.up {
            background: var(--teal-soft);
            color: var(--teal);
        }

        .metric-card__trend.down {
            background: var(--coral-soft);
            color: var(--coral);
        }

        .metric-card__caption {
            margin-top: 0.7rem;
            color: var(--ink-soft);
            font-size: 0.92rem;
            line-height: 1.5;
        }

        .subsection-title {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 1.65rem;
            line-height: 1.08;
            letter-spacing: -0.03em;
            color: var(--ink);
            margin: 0 0 0.8rem 0;
        }

        .subsection-note {
            color: var(--ink-soft);
            font-size: 0.92rem;
            line-height: 1.5;
            margin: 0 0 0.75rem 0;
        }

        .panel-card {
            padding: 1rem;
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--shadow);
        }

        .city-card {
            height: 100%;
            padding: 1rem;
            border-radius: var(--radius-md);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--shadow);
        }

        .city-card__name {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 1.02rem;
            margin-bottom: 0.35rem;
            color: var(--ink);
        }

        .city-card__headline {
            font-size: 1.75rem;
            font-weight: 800;
            color: var(--teal);
            margin-bottom: 0.5rem;
        }

        .city-card__meta {
            color: var(--ink-soft);
            font-size: 0.92rem;
            line-height: 1.58;
        }

        .ops-card {
            padding: 1rem;
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--shadow);
            margin-bottom: 0.9rem;
        }

        .ops-card__title {
            font-family: "Space Grotesk", "Aptos Display", sans-serif;
            font-size: 1.18rem;
            margin: 0 0 0.2rem 0;
            color: var(--ink);
        }

        .ops-card__subtitle {
            color: var(--ink-soft);
            font-size: 0.88rem;
            margin: 0 0 0.8rem 0;
        }

        .signal-row {
            display: flex;
            justify-content: space-between;
            gap: 0.9rem;
            padding: 0.72rem 0;
            border-top: 1px solid var(--border);
        }

        .signal-row:first-of-type {
            border-top: 0;
            padding-top: 0;
        }

        .signal-row__title {
            color: var(--ink);
            font-size: 0.93rem;
            font-weight: 700;
            line-height: 1.35;
        }

        .signal-row__meta {
            color: var(--ink-soft);
            font-size: 0.84rem;
            line-height: 1.45;
            margin-top: 0.2rem;
        }

        .signal-row__value {
            color: var(--ink);
            font-size: 0.92rem;
            font-weight: 800;
            white-space: nowrap;
        }

        .signal-row__value.teal { color: var(--teal); }
        .signal-row__value.amber { color: var(--amber); }
        .signal-row__value.coral { color: var(--coral); }

        .filter-card {
            padding: 1rem;
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            background: var(--surface);
            box-shadow: var(--shadow);
            margin: 1rem 0 1.1rem 0;
        }

        .filter-label {
            color: var(--ink-soft);
            font-size: 0.82rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.35rem;
        }

        [data-testid="stDataFrame"] {
            border-radius: var(--radius-md);
            border: 1px solid var(--border);
        }

        [data-testid="stExpander"] details {
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            background: var(--surface);
        }

        [data-testid="stExpander"] summary {
            font-weight: 700;
            color: var(--ink);
        }

        [data-testid="stRadio"] label,
        [data-testid="stMultiSelect"] label,
        [data-testid="stSelectbox"] label {
            color: var(--ink);
        }

        .stButton button {
            min-height: 44px;
            border-radius: 999px;
            border: 1px solid rgba(17, 35, 43, 0.08);
            background: linear-gradient(135deg, #11232b, #204350);
            color: #ffffff;
            font-weight: 700;
            box-shadow: var(--shadow);
        }

        .stButton button:hover {
            color: #ffffff;
            border-color: rgba(17, 35, 43, 0.08);
        }

        .footer-note {
            margin-top: 1.2rem;
            padding: 0.95rem 0 0 0;
            border-top: 1px solid var(--border);
            color: var(--ink-soft);
            font-size: 0.86rem;
            line-height: 1.5;
        }

        @media (max-width: 980px) {
            .topbar {
                padding: 1rem;
            }

            .metric-card {
                min-height: 142px;
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
    except (SQLAlchemyError, ModuleNotFoundError, ImportError, OSError) as exc:
        LOGGER.warning("Dashboard query failed: %s", exc)
        return pd.DataFrame()
    except Exception as exc:  # pragma: no cover - defensive guard for local runtime issues
        LOGGER.warning("Dashboard query failed: %s", exc)
        return pd.DataFrame()


def load_overview_metrics() -> dict[str, float | int]:
    metrics = safe_query(
        """
        WITH current_metrics AS (
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
            alert_metrics.alert_count_1h
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


def ensure_frame_schema(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    normalized = frame.copy()
    for column in columns:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    return normalized.loc[:, list(columns)]


def load_dashboard_payload(settings: Settings) -> DashboardPayload:
    return DashboardPayload(
        metrics=load_overview_metrics(),
        freshness=load_data_freshness(),
        latest_status=ensure_frame_schema(load_latest_station_status(), LATEST_STATUS_COLUMNS),
        top_stations=ensure_frame_schema(
            load_top_utilization_stations(settings.top_station_limit),
            TOP_STATIONS_COLUMNS,
        ),
        city_summary=ensure_frame_schema(load_city_summary(), CITY_SUMMARY_COLUMNS),
        low_availability=ensure_frame_schema(load_low_availability_zones(), LOW_AVAILABILITY_COLUMNS),
        peaks=ensure_frame_schema(load_daily_usage_peaks(), PEAKS_COLUMNS),
        critical=ensure_frame_schema(load_critical_stations(), CRITICAL_COLUMNS),
        alerts=ensure_frame_schema(load_station_alerts(), ALERTS_COLUMNS),
        imbalance=ensure_frame_schema(load_geographic_imbalance(), IMBALANCE_COLUMNS),
    )


def zero_metric_deltas() -> dict[str, float | int]:
    return {
        "total_stations": 0,
        "avg_utilization_rate": 0.0,
        "empty_stations": 0,
        "critical_station_count": 0,
    }


def compute_metric_deltas(
    current_metrics: dict[str, float | int],
    previous_metrics: dict[str, float | int] | None,
) -> dict[str, float | int]:
    if not previous_metrics:
        return zero_metric_deltas()

    return {
        "total_stations": int(current_metrics["total_stations"]) - int(previous_metrics["total_stations"]),
        "avg_utilization_rate": float(current_metrics["avg_utilization_rate"])
        - float(previous_metrics["avg_utilization_rate"]),
        "empty_stations": int(current_metrics["empty_stations"]) - int(previous_metrics["empty_stations"]),
        "critical_station_count": int(current_metrics["critical_station_count"])
        - int(previous_metrics["critical_station_count"]),
    }


def advance_metric_state(
    current_snapshot_token: str | None,
    current_metrics: dict[str, float | int],
    previous_snapshot_token: str | None,
    previous_metrics: dict[str, float | int] | None,
    stored_deltas: dict[str, float | int] | None,
) -> tuple[str | None, dict[str, float | int], dict[str, float | int], bool]:
    existing_deltas = stored_deltas or zero_metric_deltas()

    if current_snapshot_token is None:
        return (
            previous_snapshot_token,
            current_metrics,
            existing_deltas,
            False,
        )

    if previous_snapshot_token != current_snapshot_token:
        return (
            current_snapshot_token,
            current_metrics,
            compute_metric_deltas(current_metrics, previous_metrics),
            True,
        )

    return (
        previous_snapshot_token,
        previous_metrics or current_metrics,
        existing_deltas,
        False,
    )


def to_snapshot_token(value: pd.Timestamp | None) -> str | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC").isoformat()


def sync_metric_state(
    current_metrics: dict[str, float | int],
    latest_snapshot: pd.Timestamp | None,
) -> tuple[dict[str, float | int], bool]:
    snapshot_token = to_snapshot_token(latest_snapshot)
    next_snapshot, next_metrics, next_deltas, snapshot_changed = advance_metric_state(
        snapshot_token,
        current_metrics,
        st.session_state.get("dashboard_snapshot_token"),
        st.session_state.get("dashboard_previous_metrics"),
        st.session_state.get("dashboard_metric_deltas"),
    )
    st.session_state["dashboard_snapshot_token"] = next_snapshot
    st.session_state["dashboard_previous_metrics"] = next_metrics
    st.session_state["dashboard_metric_deltas"] = next_deltas
    return next_deltas, snapshot_changed


def normalize_selected_cities(
    city_options: list[str],
    selected_cities: Iterable[str] | None,
) -> list[str]:
    valid_selection = [
        city for city in (selected_cities or []) if city in city_options
    ]
    return valid_selection or city_options


def initialize_ui_state(city_options: list[str]) -> None:
    st.session_state.setdefault(
        "dashboard_selected_cities",
        normalize_selected_cities(city_options, city_options),
    )
    st.session_state.setdefault(
        CITY_FILTER_WIDGET_KEY,
        normalize_selected_cities(
            city_options,
            st.session_state.get("dashboard_selected_cities"),
        ),
    )
    st.session_state["dashboard_selected_cities"] = normalize_selected_cities(
        city_options,
        st.session_state.get("dashboard_selected_cities"),
    )
    st.session_state.setdefault("dashboard_map_mode", MAP_MODE_OPTIONS[0])
    st.session_state.setdefault("dashboard_station_focus", STATION_FOCUS_OPTIONS[0])
    st.session_state.setdefault("dashboard_snapshot_token", None)
    st.session_state.setdefault("dashboard_previous_metrics", None)
    st.session_state.setdefault("dashboard_metric_deltas", zero_metric_deltas())


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
        return df.copy()
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


def format_timestamp_display(value: pd.Timestamp | Any) -> str:
    timestamp = to_local_timestamp(value)
    if timestamp is None:
        return "-"
    return timestamp.strftime("%d/%m/%Y %H:%M:%S")


def format_count(value: int | float) -> str:
    return f"{int(value):,}".replace(",", " ")


def format_rate(value: float) -> str:
    return f"{float(value):.1%}"


def format_refresh_interval(seconds: int) -> str:
    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"{hours} h" if hours > 1 else "1 h"
    if seconds % 60 == 0:
        return f"{seconds // 60} min"
    return f"{seconds} s"


def format_signed_delta(delta: int | float, suffix: str = "") -> str:
    if isinstance(delta, float):
        if abs(delta) < 0.0005:
            return "Stable"
        base = f"{delta * 100:+.1f}"
        return f"{base} {suffix}".strip()
    if delta == 0:
        return "Stable"
    base = f"{delta:+d}"
    return f"{base} {suffix}".strip()


def describe_delta_style(delta: int | float) -> str:
    if isinstance(delta, float):
        if delta > 0.0005:
            return "up"
        if delta < -0.0005:
            return "down"
        return "neutral"
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "neutral"


def localize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

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
            "live",
            f"Dernier snapshot il y a {age_minutes} min {remaining_seconds:02d} s.",
        )

    return (
        "warning",
        "Flux ancien ou limite. "
        f"Dernier snapshot il y a {age_minutes} min {remaining_seconds:02d} s.",
    )


def build_live_status(
    freshness: dict[str, Any],
    poll_interval_seconds: int,
) -> dict[str, str]:
    latest_snapshot = freshness.get("latest_snapshot")
    latest_ingested = freshness.get("latest_ingested")
    level, message = describe_freshness(
        latest_snapshot,
        poll_interval_seconds,
    )
    return {
        "level": level,
        "message": message,
        "latest_snapshot": format_timestamp_display(latest_snapshot),
        "latest_ingested": format_timestamp_display(latest_ingested),
        "cadence": f"Cadence attendue: {poll_interval_seconds} s",
    }


def build_station_map_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = prepare_map_frame(df)
    if frame.empty:
        return frame

    frame = frame.copy()
    frame["utilization_label"] = frame["utilization_rate"].map(format_rate)
    frame["snapshot_label"] = frame["snapshot_timestamp"].map(format_timestamp_display)
    frame["radius"] = frame["capacity"].clip(lower=4).fillna(4) * 20

    colors: list[list[int]] = []
    states: list[str] = []
    for row in frame.itertuples(index=False):
        if row.bikes_available <= 0:
            colors.append([200, 85, 61, 205])
            states.append("vide")
        elif row.utilization_rate >= 0.85 or row.bikes_available <= 2:
            colors.append([183, 121, 31, 205])
            states.append("sous tension")
        else:
            colors.append([15, 118, 110, 185])
            states.append("normal")
    frame["fill_color"] = colors
    frame["station_state"] = states
    return frame


def build_city_card_markup(city_row: dict[str, Any]) -> str:
    return (
        '<article class="city-card">'
        f'<div class="city-card__name">{html.escape(str(city_row["city"]))}</div>'
        f'<div class="city-card__headline">{float(city_row["avg_utilization_rate"]):.1%}</div>'
        '<div class="city-card__meta">'
        f'{int(city_row["station_count"])} stations<br/>'
        f'{float(city_row["avg_bikes_available"]):.1f} velos en moyenne<br/>'
        f'{int(city_row["empty_stations"])} station(s) vide(s)<br/>'
        f'{int(city_row["low_stock_stations"])} station(s) sous tension'
        "</div>"
        "</article>"
    )


def chunk_records(records: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [records[index : index + size] for index in range(0, len(records), size)]


def render_section_label(text: str) -> None:
    st.markdown(f'<div class="section-label">{html.escape(text)}</div>', unsafe_allow_html=True)


def render_muted_text(text: str) -> None:
    st.markdown(f'<p class="subsection-note">{html.escape(text)}</p>', unsafe_allow_html=True)


def render_subsection_title(title: str) -> None:
    st.markdown(f'<div class="subsection-title">{html.escape(title)}</div>', unsafe_allow_html=True)


def render_metric_card(
    container: Any,
    *,
    eyebrow: str,
    value: str,
    trend_text: str,
    trend_style: str,
    caption: str,
    accent: str,
) -> None:
    container.markdown(
        f"""
        <section class="metric-card">
            <div class="metric-card__bar {html.escape(accent)}"></div>
            <div class="metric-card__eyebrow">{html.escape(eyebrow)}</div>
            <div class="metric-card__value">{html.escape(value)}</div>
            <div class="metric-card__trend {html.escape(trend_style)}">{html.escape(trend_text)}</div>
            <div class="metric-card__caption">{html.escape(caption)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_status_badge(text: str, *, tone: str) -> None:
    st.markdown(
        f'<span class="status-badge {html.escape(tone)}">{html.escape(text)}</span>',
        unsafe_allow_html=True,
    )


def render_signal_item(
    *,
    title: str,
    meta: str,
    value: str,
    tone: str = "teal",
) -> None:
    st.markdown(
        f"""
        <div class="signal-row">
            <div>
                <div class="signal-row__title">{html.escape(title)}</div>
                <div class="signal-row__meta">{html.escape(meta)}</div>
            </div>
            <div class="signal-row__value {html.escape(tone)}">{html.escape(value)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_topbar(
    settings: Settings,
    metrics: dict[str, float | int],
    status: dict[str, str],
    selected_cities: list[str],
    available_city_count: int,
) -> None:
    left_col, right_col = st.columns([6, 1.5], vertical_alignment="bottom")

    with left_col:
        st.title("CityBike France")
        st.caption(
            "Vue simple du reseau pour suivre les stations, les alertes et les zones en tension."
        )

    with right_col:
        if st.button("Actualiser", key="dashboard_manual_refresh", use_container_width=True):
            st.rerun()

    badge_cols = st.columns([2.8, 2.1, 2.1, 1.9], vertical_alignment="center")
    with badge_cols[0]:
        render_status_badge(status["message"], tone=status["level"])
    with badge_cols[1]:
        render_status_badge(f"Snapshot: {status['latest_snapshot']}", tone="info")
    with badge_cols[2]:
        render_status_badge(f"Ingestion: {status['latest_ingested']}", tone="info")
    with badge_cols[3]:
        render_status_badge(
            f"Scope: {len(selected_cities)}/{available_city_count} villes",
            tone="info",
        )
    st.caption(
        f"{metrics['total_stations']} stations chargees · refresh auto toutes les "
        f"{format_refresh_interval(settings.dashboard_refresh_seconds)} · {status['cadence']}"
    )


def render_metric_ribbon(
    metrics: dict[str, float | int],
    metric_deltas: dict[str, float | int],
    snapshot_changed: bool,
) -> None:
    render_section_label("KPI temps reel")
    first_row = st.columns(3)
    second_row = st.columns(3)

    first_row[0].metric(
        "Stations suivies",
        format_count(metrics["total_stations"]),
        delta=(
            format_signed_delta(int(metric_deltas["total_stations"]), "stations")
            if snapshot_changed
            else "Stable"
        ),
    )
    first_row[1].metric(
        "Villes actives",
        format_count(metrics["tracked_cities"]),
        delta="Scope actif",
    )
    first_row[2].metric(
        "Utilisation moyenne",
        format_rate(float(metrics["avg_utilization_rate"])),
        delta=(
            format_signed_delta(float(metric_deltas["avg_utilization_rate"]), "pts")
            if snapshot_changed
            else "Stable"
        ),
    )
    second_row[0].metric(
        "Stations vides",
        format_count(metrics["empty_stations"]),
        delta=(
            format_signed_delta(int(metric_deltas["empty_stations"]), "stations")
            if snapshot_changed
            else "Stable"
        ),
        delta_color="inverse",
    )
    second_row[1].metric(
        "Stations critiques",
        format_count(metrics["critical_station_count"]),
        delta=(
            format_signed_delta(int(metric_deltas["critical_station_count"]), "stations")
            if snapshot_changed
            else "Stable"
        ),
        delta_color="inverse",
    )
    second_row[2].metric(
        "Alertes 24 h",
        format_count(metrics["alert_count_24h"]),
        delta=f"{format_count(metrics['alert_count_1h'])} sur 1 h",
        delta_color="inverse",
    )


def render_station_map(latest_status: pd.DataFrame) -> None:
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
        stroked=True,
        get_line_color=[17, 35, 43, 60],
        line_width_min_pixels=1,
    )
    view_state = pdk.ViewState(
        latitude=float(map_frame["lat"].mean()),
        longitude=float(map_frame["lon"].mean()),
        zoom=5.15,
        pitch=18,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[scatter_layer],
            initial_view_state=view_state,
            tooltip={
                "html": (
                    "<b>{station_name}</b><br/>"
                    "{city}<br/>"
                    "Etat: {station_state}<br/>"
                    "Velos: {bikes_available} | Bornes: {free_slots}<br/>"
                    "Utilisation: {utilization_label}<br/>"
                    "Snapshot: {snapshot_label}"
                )
            },
        ),
        use_container_width=True,
    )


def render_heatmap(latest_status: pd.DataFrame) -> None:
    heatmap_df = prepare_map_frame(latest_status)
    if heatmap_df.empty:
        st.info("La heatmap apparaitra apres le premier chargement de donnees.")
        return

    heatmap_layer = pdk.Layer(
        "HeatmapLayer",
        data=heatmap_df,
        get_position="[lon, lat]",
        get_weight="utilization_rate",
        radiusPixels=58,
        intensity=1.05,
        threshold=0.08,
    )
    view_state = pdk.ViewState(
        latitude=float(heatmap_df["lat"].mean()),
        longitude=float(heatmap_df["lon"].mean()),
        zoom=5.1,
        pitch=25,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[heatmap_layer],
            initial_view_state=view_state,
        ),
        use_container_width=True,
    )


def render_map_panel(
    latest_status: pd.DataFrame,
    station_focus: str,
) -> None:
    st.subheader("Carte reseau")
    st.caption("Vue terrain du reseau selon les filtres actifs.")
    st.radio(
        "Mode carte",
        MAP_MODE_OPTIONS,
        key="dashboard_map_mode",
        label_visibility="collapsed",
        horizontal=True,
    )

    if st.session_state["dashboard_map_mode"] == "Stations":
        render_station_map(latest_status)
    else:
        render_heatmap(latest_status)

    st.caption(
        f"{len(latest_status)} station(s) visibles avec le focus '{station_focus.lower()}'."
    )


def render_ops_rail(
    metrics: dict[str, float | int],
    critical: pd.DataFrame,
    low_availability: pd.DataFrame,
    top_stations: pd.DataFrame,
) -> None:
    render_section_label("Ops rail")

    with st.container(border=True):
        st.markdown(
            """
            <div class="ops-card__title">Signaux rapides</div>
            <div class="ops-card__subtitle">Points d'attention immediats pour l'exploitation.</div>
            """,
            unsafe_allow_html=True,
        )
        signal_cols = st.columns(2)
        render_metric_card(
            signal_cols[0],
            eyebrow="Alertes 1 h",
            value=format_count(metrics["alert_count_1h"]),
            trend_text="Glissant sur 60 min",
            trend_style="neutral",
            caption="Nouvelles alertes detectees recemment.",
            accent="coral",
        )
        render_metric_card(
            signal_cols[1],
            eyebrow="Critiques live",
            value=format_count(len(critical)),
            trend_text="Etat courant",
            trend_style="neutral",
            caption="Stations critiques presentes dans la couche or.",
            accent="amber",
        )

    with st.container(border=True):
        st.markdown(
            """
            <div class="ops-card__title">Stations critiques</div>
            <div class="ops-card__subtitle">Les cas les plus urgents sur le dernier snapshot.</div>
            """,
            unsafe_allow_html=True,
        )
        if critical.empty:
            st.info("Aucune station critique active.")
        else:
            for row in critical.head(5).itertuples(index=False):
                render_signal_item(
                    title=f"{row.city} · {row.station_name}",
                    meta=f"{row.bikes_available} velo(s) | util. glissante {row.rolling_15m_utilization:.1%}",
                    value="critique",
                    tone="coral",
                )

    shortage = low_availability[low_availability["shortage_flag"]] if not low_availability.empty else low_availability
    with st.container(border=True):
        st.markdown(
            """
            <div class="ops-card__title">Zones en tension</div>
            <div class="ops-card__subtitle">Agrégation par zone avec disponibilité faible.</div>
            """,
            unsafe_allow_html=True,
        )
        if shortage.empty:
            st.info("Aucune zone en tension n'est remontee.")
        else:
            for row in shortage.head(5).itertuples(index=False):
                render_signal_item(
                    title=f"{row.city} · {row.zone_id}",
                    meta=f"{row.station_count} station(s) | {row.avg_bikes_available:.1f} velo(s) moyens",
                    value=f"{row.avg_utilization_rate:.0%}",
                    tone="amber",
                )

    with st.container(border=True):
        st.markdown(
            """
            <div class="ops-card__title">Stations saturees</div>
            <div class="ops-card__subtitle">Top utilisation sur l'etat consolide actuel.</div>
            """,
            unsafe_allow_html=True,
        )
        if top_stations.empty:
            st.info("Pas encore de top stations consolide.")
        else:
            for row in top_stations.head(5).itertuples(index=False):
                render_signal_item(
                    title=f"{row.city} · {row.station_name}",
                    meta=f"{row.bikes_available} velo(s) | capacite {row.capacity}",
                    value=f"{row.utilization_rate:.0%}",
                    tone="teal",
                )


def render_city_health(city_summary: pd.DataFrame) -> None:
    render_section_label("Sante par ville")
    render_subsection_title("Lecture locale du reseau")
    render_muted_text(
        "Comparatif compact des villes pour reperer rapidement les ecarts de charge et les poches de tension."
    )

    if city_summary.empty:
        st.info("Les cartes ville apparaitront des que les vues analytiques seront chargees.")
        return

    records = city_summary.to_dict(orient="records")
    for row_chunk in chunk_records(records, 3):
        columns = st.columns(len(row_chunk))
        for column, record in zip(columns, row_chunk):
            column.markdown(build_city_card_markup(record), unsafe_allow_html=True)


def build_activity_chart(peaks: pd.DataFrame) -> alt.Chart | None:
    if peaks.empty:
        return None

    chart_frame = peaks.copy()
    chart_frame["period_start"] = pd.to_datetime(chart_frame["usage_date"]) + pd.to_timedelta(
        chart_frame["usage_hour"],
        unit="h",
    )
    chart_frame["city"] = chart_frame["city"].astype(str)

    return (
        alt.Chart(chart_frame)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("period_start:T", title="Temps"),
            y=alt.Y("estimated_activity:Q", title="Activite estimee"),
            color=alt.Color(
                "city:N",
                title="Ville",
                scale=alt.Scale(
                    range=["#0f766e", "#1d4ed8", "#b7791f", "#c8553d", "#334c55"]
                ),
            ),
            tooltip=[
                alt.Tooltip("city:N", title="Ville"),
                alt.Tooltip("period_start:T", title="Periode"),
                alt.Tooltip("estimated_activity:Q", title="Activite", format=".2f"),
                alt.Tooltip("peak_rank:Q", title="Rang"),
            ],
        )
        .properties(height=320, background="#ffffff")
        .configure_view(fill="#ffffff", strokeOpacity=0)
        .configure_axis(
            labelColor="#5f747c",
            titleColor="#11232b",
            gridColor="#e8eeec",
        )
        .configure_legend(
            labelColor="#11232b",
            titleColor="#11232b",
            orient="top",
        )
    )


def build_imbalance_chart(imbalance: pd.DataFrame) -> alt.Chart | None:
    if imbalance.empty:
        return None

    chart_frame = imbalance.copy().head(12)
    chart_frame["direction"] = chart_frame["imbalance_score"].map(
        lambda value: "Sous-dotee" if value < 0 else "Sur-dotee"
    )
    chart_frame["zone_label"] = chart_frame["city"] + " · " + chart_frame["zone_id"]

    return (
        alt.Chart(chart_frame)
        .mark_bar(cornerRadiusEnd=6)
        .encode(
            x=alt.X("imbalance_score:Q", title="Score de desequilibre"),
            y=alt.Y("zone_label:N", sort="-x", title=None),
            color=alt.Color(
                "direction:N",
                scale=alt.Scale(
                    domain=["Sur-dotee", "Sous-dotee"],
                    range=["#0f766e", "#c8553d"],
                ),
                title="Lecture",
            ),
            tooltip=[
                alt.Tooltip("city:N", title="Ville"),
                alt.Tooltip("zone_id:N", title="Zone"),
                alt.Tooltip("imbalance_score:Q", title="Score", format=".3f"),
                alt.Tooltip("avg_bikes_available:Q", title="Velos moyens", format=".2f"),
                alt.Tooltip("avg_capacity:Q", title="Capacite moyenne", format=".2f"),
            ],
        )
        .properties(height=320, background="#ffffff")
        .configure_view(fill="#ffffff", strokeOpacity=0)
        .configure_axis(
            labelColor="#5f747c",
            titleColor="#11232b",
            gridColor="#e8eeec",
        )
        .configure_legend(
            labelColor="#11232b",
            titleColor="#11232b",
            orient="top",
        )
    )


def render_analytics(peaks: pd.DataFrame, imbalance: pd.DataFrame) -> None:
    render_section_label("Analytics")
    analytics_left, analytics_right = st.columns(2)

    with analytics_left:
        render_subsection_title("Activite estimee")
        render_muted_text("Inference horaire a partir des variations de stock capturees.")
        activity_chart = build_activity_chart(peaks)
        if activity_chart is None:
            st.info("L'activite apparaitra quand l'historique de snapshots sera suffisant.")
        else:
            st.altair_chart(activity_chart, use_container_width=True)

    with analytics_right:
        render_subsection_title("Desequilibre geographique")
        render_muted_text("Zones relativement sur-dotees ou sous-dotees par rapport a leur ville.")
        imbalance_chart = build_imbalance_chart(imbalance)
        if imbalance_chart is None:
            st.info("Le desequilibre geographique apparaitra apres le premier calcul analytique.")
        else:
            st.altair_chart(imbalance_chart, use_container_width=True)


def render_drilldown_details(
    latest_status: pd.DataFrame,
    top_stations: pd.DataFrame,
    low_availability: pd.DataFrame,
    alerts: pd.DataFrame,
) -> None:
    render_section_label("Details")
    render_subsection_title("Drill-down operationnel")
    render_muted_text(
        "Les tableaux restent disponibles en second niveau pour conserver une interface propre tout en gardant la profondeur d'analyse."
    )

    left_col, right_col = st.columns(2)

    with left_col:
        with st.expander("Stations visibles", expanded=False):
            if latest_status.empty:
                st.info("Aucune station visible avec les filtres actifs.")
            else:
                st.dataframe(
                    localize_frame(
                        latest_status[
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

        with st.expander("Top utilisation", expanded=False):
            if top_stations.empty:
                st.info("Aucune station a forte utilisation n'est encore disponible.")
            else:
                st.dataframe(
                    localize_frame(top_stations),
                    use_container_width=True,
                    hide_index=True,
                )

    with right_col:
        with st.expander("Zones a faible disponibilite", expanded=False):
            if low_availability.empty:
                st.info("Aucune zone a faible disponibilite.")
            else:
                focus_frame = low_availability.copy()
                if "shortage_flag" in focus_frame.columns:
                    focus_frame = focus_frame[focus_frame["shortage_flag"]]
                st.dataframe(
                    localize_frame(focus_frame),
                    use_container_width=True,
                    hide_index=True,
                )

        with st.expander("Alertes recentes", expanded=False):
            if alerts.empty:
                st.info("Aucune alerte recente n'a encore ete capturee.")
            else:
                st.dataframe(
                    localize_frame(alerts),
                    use_container_width=True,
                    hide_index=True,
                )


def render_footer(status: dict[str, str]) -> None:
    st.markdown(
        f"""
        <div class="footer-note">
            Source de donnees: CityBikes API. Etat du flux: {html.escape(status["message"])}
            · Dernier snapshot: {html.escape(status["latest_snapshot"])}
            · Rafraichissement automatique actif.
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_ops_rail(
    metrics: dict[str, float | int],
    critical: pd.DataFrame,
    low_availability: pd.DataFrame,
    top_stations: pd.DataFrame,
) -> None:
    st.subheader("A surveiller")
    quick_cols = st.columns(2)
    quick_cols[0].metric("Alertes 1 h", format_count(metrics["alert_count_1h"]), delta="glissant")
    quick_cols[1].metric(
        "Stations critiques",
        format_count(len(critical)),
        delta="live",
        delta_color="inverse",
    )

    st.markdown("**Stations critiques**")
    if critical.empty:
        st.info("Aucune station critique active.")
    else:
        critical_view = critical.head(5)[
            ["city", "station_name", "bikes_available", "rolling_15m_utilization"]
        ].copy()
        st.dataframe(localize_frame(critical_view), use_container_width=True, hide_index=True)

    st.markdown("**Zones en tension**")
    shortage = low_availability[low_availability["shortage_flag"]] if not low_availability.empty else low_availability
    if shortage.empty:
        st.info("Aucune zone en tension.")
    else:
        shortage_view = shortage.head(5)[
            ["city", "zone_id", "station_count", "avg_bikes_available", "avg_utilization_rate"]
        ].copy()
        st.dataframe(localize_frame(shortage_view), use_container_width=True, hide_index=True)

    st.markdown("**Top saturation**")
    if top_stations.empty:
        st.info("Pas encore de top stations consolide.")
    else:
        top_view = top_stations.head(5)[
            ["city", "station_name", "bikes_available", "capacity", "utilization_rate"]
        ].copy()
        st.dataframe(localize_frame(top_view), use_container_width=True, hide_index=True)


def render_city_health(city_summary: pd.DataFrame) -> None:
    render_section_label("Villes")
    st.subheader("Vue par ville")
    st.caption("Lecture simple des indicateurs locaux.")

    if city_summary.empty:
        st.info("Le tableau ville apparaitra des que les vues analytiques seront chargees.")
        return

    city_view = city_summary[
        [
            "city",
            "station_count",
            "avg_utilization_rate",
            "avg_bikes_available",
            "empty_stations",
            "low_stock_stations",
        ]
    ].copy()
    st.dataframe(localize_frame(city_view), use_container_width=True, hide_index=True)


def render_analytics(peaks: pd.DataFrame, imbalance: pd.DataFrame) -> None:
    render_section_label("Analyses")
    analytics_left, analytics_right = st.columns(2)

    with analytics_left:
        st.subheader("Activite estimee")
        st.caption("Inference horaire a partir des variations de stock.")
        activity_chart = build_activity_chart(peaks)
        if activity_chart is None:
            st.info("L'activite apparaitra quand l'historique de snapshots sera suffisant.")
        else:
            st.altair_chart(activity_chart, use_container_width=True)

    with analytics_right:
        st.subheader("Desequilibre geographique")
        st.caption("Comparaison des zones par rapport a leur moyenne ville.")
        imbalance_chart = build_imbalance_chart(imbalance)
        if imbalance_chart is None:
            st.info("Le desequilibre geographique apparaitra apres le premier calcul analytique.")
        else:
            st.altair_chart(imbalance_chart, use_container_width=True)


def render_drilldown_details(
    latest_status: pd.DataFrame,
    top_stations: pd.DataFrame,
    low_availability: pd.DataFrame,
    alerts: pd.DataFrame,
) -> None:
    render_section_label("Details")
    st.subheader("Tables de detail")
    st.caption("Les tableaux complets restent accessibles en second niveau.")

    left_col, right_col = st.columns(2)

    with left_col:
        with st.expander("Stations visibles", expanded=False):
            if latest_status.empty:
                st.info("Aucune station visible avec les filtres actifs.")
            else:
                st.dataframe(
                    localize_frame(
                        latest_status[
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

        with st.expander("Top utilisation", expanded=False):
            if top_stations.empty:
                st.info("Aucune station a forte utilisation n'est encore disponible.")
            else:
                st.dataframe(localize_frame(top_stations), use_container_width=True, hide_index=True)

    with right_col:
        with st.expander("Zones a faible disponibilite", expanded=False):
            if low_availability.empty:
                st.info("Aucune zone a faible disponibilite.")
            else:
                focus_frame = low_availability.copy()
                if "shortage_flag" in focus_frame.columns:
                    focus_frame = focus_frame[focus_frame["shortage_flag"]]
                st.dataframe(localize_frame(focus_frame), use_container_width=True, hide_index=True)

        with st.expander("Alertes recentes", expanded=False):
            if alerts.empty:
                st.info("Aucune alerte recente n'a encore ete capturee.")
            else:
                st.dataframe(localize_frame(alerts), use_container_width=True, hide_index=True)


def render_footer(status: dict[str, str]) -> None:
    st.caption(
        f"Source: CityBikes API · Etat du flux: {status['message']} · "
        f"Dernier snapshot: {status['latest_snapshot']}"
    )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            max-width: 1320px;
            padding-top: 1rem;
            padding-bottom: 2rem;
        }

        [data-testid="stMetric"] {
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 14px;
            padding: 0.75rem 0.85rem;
            background: rgba(255, 255, 255, 0.02);
        }

        [data-testid="stDataFrame"],
        [data-testid="stExpander"] details {
            border-radius: 12px;
        }

        [data-testid="stVegaLiteChart"] {
            background: #ffffff;
            border: 1px solid rgba(49, 51, 63, 0.12);
            border-radius: 12px;
            padding: 0.35rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_topbar(
    settings: Settings,
    metrics: dict[str, float | int],
    status: dict[str, str],
    selected_cities: list[str],
    available_city_count: int,
) -> None:
    title_col, action_col = st.columns([6, 1.3], vertical_alignment="bottom")

    with title_col:
        st.title("CityBike France")
        st.caption("Dashboard simple pour suivre le reseau, les alertes et les zones en tension.")

    with action_col:
        if st.button("Actualiser", key="dashboard_manual_refresh", use_container_width=True):
            st.rerun()

    if status["level"] == "warning":
        st.warning(status["message"])
    elif status["level"] == "live":
        st.success(status["message"])
    else:
        st.info(status["message"])

    meta_cols = st.columns(4)
    meta_cols[0].caption(f"Snapshot: {status['latest_snapshot']}")
    meta_cols[1].caption(f"Ingestion: {status['latest_ingested']}")
    meta_cols[2].caption(f"Scope: {len(selected_cities)}/{available_city_count} villes")
    meta_cols[3].caption(f"Refresh: {format_refresh_interval(settings.dashboard_refresh_seconds)}")
    st.caption(
        f"{metrics['total_stations']} stations chargees · {status['cadence']}"
    )


def render_dashboard(settings: Settings) -> None:
    payload = load_dashboard_payload(settings)

    city_options = sorted(
        payload.latest_status.get("city", pd.Series(dtype="object")).dropna().unique().tolist()
    )
    if not city_options:
        city_options = list(settings.target_cities)

    initialize_ui_state(city_options)
    selected_cities = st.session_state["dashboard_selected_cities"]
    status = build_live_status(payload.freshness, settings.poll_interval_seconds)
    metric_deltas, snapshot_changed = sync_metric_state(
        payload.metrics,
        payload.freshness["latest_snapshot"],
    )

    render_topbar(
        settings,
        payload.metrics,
        status,
        selected_cities,
        len(city_options),
    )
    render_metric_ribbon(payload.metrics, metric_deltas, snapshot_changed)

    with st.container(border=True):
        render_section_label("Filtres")
        filter_left, filter_center, filter_right = st.columns([2.3, 1.5, 1.0])
        filter_left.markdown('<div class="filter-label">Villes explorees</div>', unsafe_allow_html=True)
        selected_cities = filter_left.multiselect(
            "Villes explorees",
            options=city_options,
            key=CITY_FILTER_WIDGET_KEY,
            label_visibility="collapsed",
            help="Conserve les villes selectionnees pendant les auto-reruns.",
        )
        selected_cities = normalize_selected_cities(city_options, selected_cities)
        st.session_state["dashboard_selected_cities"] = selected_cities

        filter_center.markdown('<div class="filter-label">Focus stations</div>', unsafe_allow_html=True)
        filter_center.selectbox(
            "Focus stations",
            STATION_FOCUS_OPTIONS,
            key="dashboard_station_focus",
            label_visibility="collapsed",
            help="Affiche toutes les stations ou seulement les poches de tension.",
        )

        filter_right.markdown('<div class="filter-label">Rafraichissement</div>', unsafe_allow_html=True)
        filter_right.markdown(
            f'<p class="subsection-note">Auto toutes les {format_refresh_interval(settings.dashboard_refresh_seconds)}.</p>',
            unsafe_allow_html=True,
        )

    station_focus = st.session_state["dashboard_station_focus"]

    filtered_latest_status = filter_by_cities(payload.latest_status, selected_cities)
    focused_latest_status = apply_station_focus(filtered_latest_status, station_focus)
    filtered_top_stations = filter_by_cities(payload.top_stations, selected_cities)
    filtered_city_summary = filter_by_cities(payload.city_summary, selected_cities)
    filtered_low_availability = filter_by_cities(payload.low_availability, selected_cities)
    filtered_peaks = filter_by_cities(payload.peaks, selected_cities)
    filtered_critical = filter_by_cities(payload.critical, selected_cities)
    filtered_alerts = filter_by_cities(payload.alerts, selected_cities)
    filtered_imbalance = filter_by_cities(payload.imbalance, selected_cities)

    network_col, ops_col = st.columns([8, 4])
    with network_col:
        render_map_panel(focused_latest_status, station_focus)
    with ops_col:
        render_ops_rail(
            payload.metrics,
            filtered_critical,
            filtered_low_availability,
            filtered_top_stations,
        )

    st.divider()
    render_city_health(filtered_city_summary)
    st.divider()
    render_analytics(filtered_peaks, filtered_imbalance)
    st.divider()
    render_drilldown_details(
        focused_latest_status,
        filtered_top_stations,
        filtered_low_availability,
        filtered_alerts,
    )
    render_footer(status)


def main() -> None:
    configure_logging()
    settings = load_settings()

    st.set_page_config(
        page_title="CityBike France",
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
