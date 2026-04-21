from __future__ import annotations

import pandas as pd

from dashboard.app import prepare_map_frame


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
