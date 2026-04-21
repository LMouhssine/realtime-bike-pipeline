from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import load_settings


LOGGER = logging.getLogger("citybike.dashboard")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
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


def prepare_map_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    frame = df.copy()
    frame["lat"] = frame["latitude"]
    frame["lon"] = frame["longitude"]
    return frame.dropna(subset=["lat", "lon"])


def load_overview_metrics() -> dict[str, float | int]:
    latest = safe_query(
        """
        SELECT
            COUNT(*) AS total_stations,
            COUNT(DISTINCT city) AS tracked_cities,
            COALESCE(AVG(utilization_rate), 0) AS avg_utilization_rate,
            COALESCE(SUM(CASE WHEN bikes_available = 0 THEN 1 ELSE 0 END), 0) AS empty_stations
        FROM latest_station_status
        """
    )
    critical = safe_query("SELECT COUNT(*) AS critical_station_count FROM critical_stations")
    alerts = safe_query(
        """
        SELECT COUNT(*) AS alert_count
        FROM station_alerts
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
    )

    if latest.empty:
        return {
            "total_stations": 0,
            "tracked_cities": 0,
            "avg_utilization_rate": 0.0,
            "empty_stations": 0,
            "critical_station_count": 0,
            "alert_count": 0,
        }

    return {
        "total_stations": int(latest.loc[0, "total_stations"]),
        "tracked_cities": int(latest.loc[0, "tracked_cities"]),
        "avg_utilization_rate": float(latest.loc[0, "avg_utilization_rate"]),
        "empty_stations": int(latest.loc[0, "empty_stations"]),
        "critical_station_count": int(
            critical.loc[0, "critical_station_count"] if not critical.empty else 0
        ),
        "alert_count": int(alerts.loc[0, "alert_count"] if not alerts.empty else 0),
    }


def render_overview() -> None:
    metrics = load_overview_metrics()
    metric_columns = st.columns(5)
    metric_columns[0].metric("Tracked stations", metrics["total_stations"])
    metric_columns[1].metric("Cities", metrics["tracked_cities"])
    metric_columns[2].metric(
        "Average utilization",
        f"{metrics['avg_utilization_rate']:.1%}",
    )
    metric_columns[3].metric("Critical stations", metrics["critical_station_count"])
    metric_columns[4].metric("24h alerts", metrics["alert_count"])

    top_stations = safe_query(
        """
        SELECT city, station_name, bikes_available, free_slots, utilization_rate, snapshot_timestamp
        FROM top_utilization_stations
        ORDER BY utilization_rank ASC, station_name ASC
        LIMIT :limit
        """,
        {"limit": load_settings().top_station_limit},
    )
    if top_stations.empty:
        st.info("No station data is available yet. Start Kafka, Spark, and the producer first.")
        return

    st.subheader("Highest utilization stations")
    st.dataframe(top_stations, use_container_width=True, hide_index=True)


def render_station_map() -> None:
    latest_status = safe_query(
        """
        SELECT
            city,
            station_name,
            latitude,
            longitude,
            bikes_available,
            free_slots,
            utilization_rate,
            snapshot_timestamp
        FROM latest_station_status
        ORDER BY city, station_name
        """
    )
    map_frame = prepare_map_frame(latest_status)
    if map_frame.empty:
        st.info("Map will appear after the first successful micro-batch.")
        return

    st.map(map_frame[["lat", "lon"]], use_container_width=True)
    st.dataframe(map_frame, use_container_width=True, hide_index=True)


def render_utilization_heatmap() -> None:
    heatmap_df = safe_query(
        """
        SELECT
            latitude,
            longitude,
            utilization_rate,
            city,
            station_name
        FROM latest_station_status
        """
    )
    heatmap_df = prepare_map_frame(heatmap_df)
    if heatmap_df.empty:
        st.info("Utilization heatmap is waiting for live data.")
        return

    layer = pdk.Layer(
        "HeatmapLayer",
        data=heatmap_df,
        get_position="[lon, lat]",
        get_weight="utilization_rate",
        radiusPixels=45,
        intensity=1.4,
        threshold=0.15,
    )
    view_state = pdk.ViewState(
        latitude=float(heatmap_df["lat"].mean()),
        longitude=float(heatmap_df["lon"].mean()),
        zoom=5,
        pitch=35,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{station_name} ({city})\nUtilization: {utilization_rate}"},
        )
    )


def render_time_series() -> None:
    peaks = safe_query(
        """
        SELECT city, usage_date, usage_hour, estimated_activity, peak_rank
        FROM daily_usage_peaks
        ORDER BY usage_date DESC, city ASC, usage_hour ASC
        """
    )
    if peaks.empty:
        st.info("Usage-peak analytics will populate after historical snapshots accumulate.")
        return

    peaks["label"] = peaks["usage_date"].astype(str) + " " + peaks["usage_hour"].astype(str) + ":00"
    chart_frame = peaks.pivot_table(
        index="label",
        columns="city",
        values="estimated_activity",
        aggfunc="sum",
    ).fillna(0.0)
    st.line_chart(chart_frame, use_container_width=True)
    st.dataframe(peaks, use_container_width=True, hide_index=True)


def render_alerts() -> None:
    critical = safe_query(
        """
        SELECT
            city,
            station_name,
            bikes_available,
            rolling_15m_utilization,
            snapshot_timestamp,
            alert_level
        FROM critical_stations
        ORDER BY snapshot_timestamp DESC, city ASC
        """
    )
    alerts = safe_query(
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

    if critical.empty and alerts.empty:
        st.info("No critical or empty-station alerts have been generated yet.")
        return

    if not critical.empty:
        st.subheader("Critical stations")
        st.dataframe(critical, use_container_width=True, hide_index=True)

    if not alerts.empty:
        st.subheader("Recent alerts")
        st.dataframe(alerts, use_container_width=True, hide_index=True)


def render_geographic_imbalance() -> None:
    imbalance = safe_query(
        """
        SELECT
            city,
            zone_id,
            avg_bikes_available,
            avg_capacity,
            zone_availability_ratio,
            city_availability_ratio,
            imbalance_score
        FROM geographic_imbalance
        ORDER BY ABS(imbalance_score) DESC, city ASC
        """
    )
    if imbalance.empty:
        st.info("Geographic imbalance analysis will appear once latest station snapshots are loaded.")
        return

    st.dataframe(imbalance, use_container_width=True, hide_index=True)


def main() -> None:
    configure_logging()
    settings = load_settings()

    st.set_page_config(
        page_title="CityBike Stream Analytics",
        page_icon=":bike:",
        layout="wide",
    )
    st.title("France CityBikes Real-Time Analytics")
    st.caption(
        "Live station telemetry from CityBikes, processed through Kafka, Spark Structured Streaming, "
        f"PostgreSQL, and Parquet. Refresh the page after each {settings.poll_interval_seconds}-second cycle."
    )

    tabs = st.tabs(
        [
            "Overview",
            "Station Map",
            "Utilization Heatmap",
            "Daily Usage Peaks",
            "Geographic Imbalance",
            "Alerts",
        ]
    )

    with tabs[0]:
        render_overview()
    with tabs[1]:
        render_station_map()
    with tabs[2]:
        render_utilization_heatmap()
    with tabs[3]:
        render_time_series()
    with tabs[4]:
        render_geographic_imbalance()
    with tabs[5]:
        render_alerts()


if __name__ == "__main__":
    main()
