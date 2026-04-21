from __future__ import annotations

from streaming.spark_streaming import build_station_schema, clean_station_frame


def test_clean_station_frame_computes_metrics_and_filters_invalid_rows(spark_session) -> None:
    schema = build_station_schema()
    input_df = spark_session.createDataFrame(
        [
            (
                "123",
                "Station A",
                48.8566,
                2.3522,
                6,
                4,
                "2026-04-21T09:00:00Z",
                "velib",
                "Paris",
                "velib:123",
                "2026-04-21T09:00:01Z",
            ),
            (
                "999",
                "Broken Station",
                43.2965,
                5.3698,
                -1,
                7,
                "2026-04-21T09:00:00Z",
                "marseille",
                "Marseille",
                "marseille:999",
                "2026-04-21T09:00:01Z",
            ),
        ],
        schema=schema,
    )

    result = clean_station_frame(input_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.station_key == "velib:123"
    assert row.capacity == 10
    assert round(row.utilization_rate, 2) == 0.60
    assert row.zone_id == "48.86_2.35"


def test_clean_station_frame_handles_zero_capacity(spark_session) -> None:
    schema = build_station_schema()
    input_df = spark_session.createDataFrame(
        [
            (
                "456",
                "Station Zero",
                45.7640,
                4.8357,
                0,
                0,
                "2026-04-21T09:00:00Z",
                "velo-v",
                "Lyon",
                "velo-v:456",
                "2026-04-21T09:00:01Z",
            )
        ],
        schema=schema,
    )

    result = clean_station_frame(input_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.capacity == 0
    assert row.utilization_rate == 0.0


def test_clean_station_frame_accepts_duplicate_utc_suffix_timestamp(spark_session) -> None:
    schema = build_station_schema()
    input_df = spark_session.createDataFrame(
        [
            (
                "789",
                "Station UTC",
                44.8378,
                -0.5792,
                3,
                9,
                "2026-04-21T11:59:43.000998+00:00Z",
                "v3-bordeaux",
                "Bordeaux",
                "v3-bordeaux:789",
                "2026-04-21T11:59:44.000998+00:00Z",
            )
        ],
        schema=schema,
    )

    result = clean_station_frame(input_df).collect()

    assert len(result) == 1
    row = result[0]
    assert row.station_key == "v3-bordeaux:789"
    assert row.snapshot_timestamp is not None
    assert row.ingested_at is not None
