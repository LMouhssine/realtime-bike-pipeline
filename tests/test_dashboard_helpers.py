from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from dashboard import app
from dashboard.app import (
    DashboardPayload,
    advance_metric_state,
    apply_station_focus,
    build_city_card_markup,
    chunk_records,
    compute_metric_deltas,
    describe_freshness,
    load_dashboard_payload,
    normalize_selected_cities,
    prepare_map_frame,
)


def test_prepare_map_frame_creates_streamlit_columns() -> None:
    frame = pd.DataFrame(
        [
            {
                "station_name": "Station A",
                "latitude": 48.8566,
                "longitude": 2.3522,
            }
        ]
    )

    prepared = prepare_map_frame(frame)

    assert list(prepared["lat"]) == [48.8566]
    assert list(prepared["lon"]) == [2.3522]


def test_prepare_map_frame_drops_missing_coordinates() -> None:
    frame = pd.DataFrame(
        [
            {"station_name": "Station A", "latitude": 48.8566, "longitude": 2.3522},
            {"station_name": "Station B", "latitude": None, "longitude": 4.8357},
        ]
    )

    prepared = prepare_map_frame(frame)

    assert len(prepared) == 1
    assert prepared.iloc[0]["station_name"] == "Station A"


def test_describe_freshness_returns_warning_for_stale_data() -> None:
    level, message = describe_freshness(
        pd.Timestamp("2026-04-21T12:00:00Z"),
        60,
        now=pd.Timestamp("2026-04-21T12:05:00Z"),
    )

    assert level == "warning"
    assert "Flux ancien" in message


def test_apply_station_focus_keeps_only_critical_or_low_stock_rows() -> None:
    frame = pd.DataFrame(
        [
            {"station_name": "A", "utilization_rate": 0.30, "bikes_available": 7},
            {"station_name": "B", "utilization_rate": 0.91, "bikes_available": 4},
            {"station_name": "C", "utilization_rate": 0.42, "bikes_available": 1},
        ]
    )

    focused = apply_station_focus(frame, "Sous tension")

    assert list(focused["station_name"]) == ["B", "C"]


def test_apply_station_focus_keeps_only_empty_rows() -> None:
    frame = pd.DataFrame(
        [
            {"station_name": "A", "utilization_rate": 0.30, "bikes_available": 0},
            {"station_name": "B", "utilization_rate": 0.91, "bikes_available": 4},
        ]
    )

    focused = apply_station_focus(frame, "Stations vides")

    assert list(focused["station_name"]) == ["A"]


def test_compute_metric_deltas_returns_numeric_differences() -> None:
    current = {
        "total_stations": 100,
        "avg_utilization_rate": 0.42,
        "empty_stations": 8,
        "critical_station_count": 3,
    }
    previous = {
        "total_stations": 98,
        "avg_utilization_rate": 0.39,
        "empty_stations": 10,
        "critical_station_count": 1,
    }

    deltas = compute_metric_deltas(current, previous)

    assert deltas["total_stations"] == 2
    assert deltas["avg_utilization_rate"] == pytest.approx(0.03)
    assert deltas["empty_stations"] == -2
    assert deltas["critical_station_count"] == 2


def test_advance_metric_state_keeps_existing_deltas_when_snapshot_is_unchanged() -> None:
    snapshot, metrics, deltas, snapshot_changed = advance_metric_state(
        "2026-04-22T10:00:00+00:00",
        {"total_stations": 100, "avg_utilization_rate": 0.4, "empty_stations": 6, "critical_station_count": 1},
        "2026-04-22T10:00:00+00:00",
        {"total_stations": 95, "avg_utilization_rate": 0.35, "empty_stations": 7, "critical_station_count": 2},
        {"total_stations": 5, "avg_utilization_rate": 0.05, "empty_stations": -1, "critical_station_count": -1},
    )

    assert snapshot == "2026-04-22T10:00:00+00:00"
    assert metrics["total_stations"] == 95
    assert deltas["total_stations"] == 5
    assert snapshot_changed is False


def test_advance_metric_state_computes_new_deltas_when_snapshot_changes() -> None:
    snapshot, metrics, deltas, snapshot_changed = advance_metric_state(
        "2026-04-22T10:05:00+00:00",
        {"total_stations": 100, "avg_utilization_rate": 0.4, "empty_stations": 6, "critical_station_count": 1},
        "2026-04-22T10:00:00+00:00",
        {"total_stations": 95, "avg_utilization_rate": 0.35, "empty_stations": 7, "critical_station_count": 2},
        None,
    )

    assert snapshot == "2026-04-22T10:05:00+00:00"
    assert metrics["total_stations"] == 100
    assert deltas["total_stations"] == 5
    assert snapshot_changed is True


def test_normalize_selected_cities_falls_back_to_all_options() -> None:
    normalized = normalize_selected_cities(["Paris", "Lyon"], ["Marseille"])

    assert normalized == ["Paris", "Lyon"]


def test_chunk_records_preserves_row_order() -> None:
    chunks = chunk_records(
        [{"city": "Paris"}, {"city": "Lyon"}, {"city": "Bordeaux"}, {"city": "Marseille"}],
        3,
    )

    assert chunks == [
        [{"city": "Paris"}, {"city": "Lyon"}, {"city": "Bordeaux"}],
        [{"city": "Marseille"}],
    ]


def test_build_city_card_markup_returns_single_article() -> None:
    markup = build_city_card_markup(
        {
            "city": "Paris",
            "avg_utilization_rate": 0.41,
            "station_count": 1508,
            "avg_bikes_available": 12.5,
            "empty_stations": 82,
            "low_stock_stations": 331,
        }
    )

    assert markup.startswith("<article")
    assert markup.endswith("</article>")
    assert markup.count("<article") == 1
    assert "Paris" in markup


def test_load_dashboard_payload_handles_empty_state(monkeypatch) -> None:
    monkeypatch.setattr(app, "load_overview_metrics", lambda: {"total_stations": 0})
    monkeypatch.setattr(
        app,
        "load_data_freshness",
        lambda: {"latest_snapshot": None, "latest_ingested": None},
    )
    monkeypatch.setattr(app, "load_latest_station_status", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_top_utilization_stations", lambda limit: pd.DataFrame())
    monkeypatch.setattr(app, "load_city_summary", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_low_availability_zones", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_daily_usage_peaks", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_critical_stations", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_station_alerts", lambda: pd.DataFrame())
    monkeypatch.setattr(app, "load_geographic_imbalance", lambda: pd.DataFrame())

    payload = load_dashboard_payload(SimpleNamespace(top_station_limit=10))

    assert isinstance(payload, DashboardPayload)
    assert payload.metrics["total_stations"] == 0
    assert payload.latest_status.empty
    assert payload.alerts.empty
    assert "city" in payload.latest_status.columns
    assert "station_name" in payload.alerts.columns


def test_load_dashboard_payload_handles_seeded_state(monkeypatch) -> None:
    latest_status = pd.DataFrame([{"city": "Paris", "station_name": "A"}])
    critical = pd.DataFrame([{"city": "Paris", "station_name": "A"}])

    monkeypatch.setattr(
        app,
        "load_overview_metrics",
        lambda: {
            "total_stations": 1,
            "tracked_cities": 1,
            "avg_utilization_rate": 0.4,
            "empty_stations": 0,
            "critical_station_count": 1,
            "alert_count_24h": 2,
            "alert_count_1h": 1,
        },
    )
    monkeypatch.setattr(
        app,
        "load_data_freshness",
        lambda: {
            "latest_snapshot": pd.Timestamp("2026-04-22T10:00:00Z"),
            "latest_ingested": pd.Timestamp("2026-04-22T10:00:03Z"),
        },
    )
    monkeypatch.setattr(app, "load_latest_station_status", lambda: latest_status)
    monkeypatch.setattr(app, "load_top_utilization_stations", lambda limit: latest_status)
    monkeypatch.setattr(app, "load_city_summary", lambda: latest_status)
    monkeypatch.setattr(app, "load_low_availability_zones", lambda: latest_status)
    monkeypatch.setattr(app, "load_daily_usage_peaks", lambda: latest_status)
    monkeypatch.setattr(app, "load_critical_stations", lambda: critical)
    monkeypatch.setattr(app, "load_station_alerts", lambda: latest_status)
    monkeypatch.setattr(app, "load_geographic_imbalance", lambda: latest_status)

    payload = load_dashboard_payload(SimpleNamespace(top_station_limit=10))

    assert payload.metrics["critical_station_count"] == 1
    assert list(payload.latest_status["city"]) == ["Paris"]
    assert list(payload.critical["station_name"]) == ["A"]
