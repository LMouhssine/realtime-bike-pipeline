from __future__ import annotations

import pandas as pd

from dashboard.app import apply_station_focus, describe_freshness, prepare_map_frame


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
    assert "Les donnees sont anciennes" in message


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
