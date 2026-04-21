from __future__ import annotations

import logging
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PgConnection
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from config.settings import Settings, load_settings


LOGGER = logging.getLogger("citybike.db_writer")

STAGE_COLUMNS = [
    "station_key",
    "network_id",
    "city",
    "station_id",
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
    "event_date",
    "event_hour",
    "batch_id",
]

FACT_INSERT_SQL = """
INSERT INTO station_status_facts (
    station_key,
    network_id,
    city,
    station_id,
    station_name,
    latitude,
    longitude,
    bikes_available,
    free_slots,
    capacity,
    utilization_rate,
    zone_id,
    snapshot_timestamp,
    ingested_at,
    event_date,
    event_hour,
    batch_id
)
SELECT
    station_key,
    network_id,
    city,
    station_id,
    station_name,
    latitude,
    longitude,
    bikes_available,
    free_slots,
    capacity,
    utilization_rate,
    zone_id,
    snapshot_timestamp,
    ingested_at,
    event_date,
    event_hour,
    batch_id
FROM station_status_stage
ON CONFLICT (station_key, snapshot_timestamp) DO NOTHING;
"""


class DbWriter:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        self._schema_bootstrapped = False

    def _connect(self) -> PgConnection:
        return psycopg2.connect(
            host=self.settings.postgres_host,
            port=self.settings.postgres_port,
            dbname=self.settings.postgres_db,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
        )

    def bootstrap_schema(self) -> None:
        if self._schema_bootstrapped:
            return

        schema_sql = (Path(__file__).resolve().with_name("init.sql")).read_text(encoding="utf-8")
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(schema_sql)
            conn.commit()
        self._schema_bootstrapped = True
        LOGGER.info("Verified PostgreSQL schema bootstrap")

    def archive_batch(self, batch_df: DataFrame) -> None:
        (
            batch_df.write.mode("append")
            .partitionBy("event_date", "city")
            .parquet(self.settings.parquet_output_path)
        )
        LOGGER.info("Archived micro-batch to Parquet at %s", self.settings.parquet_output_path)

    def load_stage_table(self, batch_df: DataFrame) -> None:
        (
            batch_df.write.format("jdbc")
            .option("url", self.settings.postgres_jdbc_url)
            .option("dbtable", "station_status_stage")
            .option("user", self.settings.postgres_user)
            .option("password", self.settings.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
        LOGGER.info("Loaded micro-batch into staging table")

    def refresh_analytics(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(FACT_INSERT_SQL)
                cursor.execute("TRUNCATE latest_station_status")
                cursor.execute(
                    """
                    INSERT INTO latest_station_status
                    SELECT DISTINCT ON (station_key)
                        station_key,
                        network_id,
                        city,
                        station_id,
                        station_name,
                        latitude,
                        longitude,
                        bikes_available,
                        free_slots,
                        capacity,
                        utilization_rate,
                        zone_id,
                        snapshot_timestamp,
                        ingested_at,
                        event_date,
                        event_hour,
                        batch_id
                    FROM station_status_facts
                    ORDER BY station_key, snapshot_timestamp DESC
                    """
                )

                cursor.execute("TRUNCATE top_utilization_stations")
                cursor.execute(
                    """
                    INSERT INTO top_utilization_stations
                    SELECT
                        station_key,
                        network_id,
                        city,
                        station_id,
                        station_name,
                        latitude,
                        longitude,
                        bikes_available,
                        free_slots,
                        capacity,
                        utilization_rate,
                        zone_id,
                        snapshot_timestamp,
                        DENSE_RANK() OVER (
                            ORDER BY utilization_rate DESC, bikes_available ASC, station_name ASC
                        ) AS utilization_rank
                    FROM latest_station_status
                    """
                )

                cursor.execute("TRUNCATE low_availability_zones")
                cursor.execute(
                    """
                    INSERT INTO low_availability_zones
                    SELECT
                        city,
                        zone_id,
                        MAX(snapshot_timestamp) AS snapshot_timestamp,
                        AVG(bikes_available)::DOUBLE PRECISION AS avg_bikes_available,
                        AVG(free_slots)::DOUBLE PRECISION AS avg_free_slots,
                        AVG(utilization_rate)::DOUBLE PRECISION AS avg_utilization_rate,
                        COUNT(*)::INTEGER AS station_count,
                        (AVG(bikes_available) < %s) AS shortage_flag
                    FROM latest_station_status
                    GROUP BY city, zone_id
                    """,
                    (self.settings.low_availability_threshold,),
                )

                cursor.execute("TRUNCATE daily_usage_peaks")
                cursor.execute(
                    """
                    WITH deltas AS (
                        SELECT
                            city,
                            event_date AS usage_date,
                            event_hour AS usage_hour,
                            GREATEST(
                                ABS(
                                    bikes_available - COALESCE(
                                        LAG(bikes_available) OVER (
                                            PARTITION BY station_key
                                            ORDER BY snapshot_timestamp
                                        ),
                                        bikes_available
                                    )
                                ),
                                ABS(
                                    free_slots - COALESCE(
                                        LAG(free_slots) OVER (
                                            PARTITION BY station_key
                                            ORDER BY snapshot_timestamp
                                        ),
                                        free_slots
                                    )
                                )
                            )::DOUBLE PRECISION AS usage_proxy
                        FROM station_status_facts
                    ),
                    aggregated AS (
                        SELECT
                            city,
                            usage_date,
                            usage_hour,
                            SUM(usage_proxy)::DOUBLE PRECISION AS estimated_activity
                        FROM deltas
                        GROUP BY city, usage_date, usage_hour
                    )
                    INSERT INTO daily_usage_peaks
                    SELECT
                        city,
                        usage_date,
                        usage_hour,
                        estimated_activity,
                        DENSE_RANK() OVER (
                            PARTITION BY city, usage_date
                            ORDER BY estimated_activity DESC, usage_hour ASC
                        ) AS peak_rank
                    FROM aggregated
                    """
                )

                cursor.execute("TRUNCATE geographic_imbalance")
                cursor.execute(
                    """
                    WITH city_baseline AS (
                        SELECT
                            city,
                            AVG(
                                CASE
                                    WHEN capacity > 0
                                        THEN bikes_available::DOUBLE PRECISION / capacity
                                    ELSE 0
                                END
                            )::DOUBLE PRECISION AS city_availability_ratio
                        FROM latest_station_status
                        GROUP BY city
                    )
                    INSERT INTO geographic_imbalance
                    SELECT
                        latest.city,
                        latest.zone_id,
                        MAX(latest.snapshot_timestamp) AS snapshot_timestamp,
                        AVG(latest.bikes_available)::DOUBLE PRECISION AS avg_bikes_available,
                        AVG(latest.capacity)::DOUBLE PRECISION AS avg_capacity,
                        AVG(
                            CASE
                                WHEN latest.capacity > 0
                                    THEN latest.bikes_available::DOUBLE PRECISION / latest.capacity
                                ELSE 0
                            END
                        )::DOUBLE PRECISION AS zone_availability_ratio,
                        baseline.city_availability_ratio,
                        (
                            AVG(
                                CASE
                                    WHEN latest.capacity > 0
                                        THEN latest.bikes_available::DOUBLE PRECISION / latest.capacity
                                    ELSE 0
                                END
                            ) - baseline.city_availability_ratio
                        )::DOUBLE PRECISION AS imbalance_score
                    FROM latest_station_status AS latest
                    INNER JOIN city_baseline AS baseline
                        ON latest.city = baseline.city
                    GROUP BY latest.city, latest.zone_id, baseline.city_availability_ratio
                    """
                )

                cursor.execute("TRUNCATE critical_stations")
                cursor.execute(
                    """
                    WITH rolling_window AS (
                        SELECT
                            latest.station_key,
                            latest.city,
                            latest.station_id,
                            latest.station_name,
                            latest.snapshot_timestamp,
                            latest.bikes_available,
                            latest.free_slots,
                            latest.utilization_rate,
                            COALESCE(window_metrics.rolling_15m_utilization, latest.utilization_rate)
                                AS rolling_15m_utilization
                        FROM latest_station_status AS latest
                        LEFT JOIN LATERAL (
                            SELECT
                                AVG(utilization_rate)::DOUBLE PRECISION AS rolling_15m_utilization
                            FROM station_status_facts AS facts
                            WHERE facts.station_key = latest.station_key
                              AND facts.snapshot_timestamp BETWEEN
                                  latest.snapshot_timestamp - INTERVAL '15 minutes'
                                  AND latest.snapshot_timestamp
                        ) AS window_metrics
                        ON TRUE
                    )
                    INSERT INTO critical_stations
                    SELECT
                        station_key,
                        city,
                        station_id,
                        station_name,
                        snapshot_timestamp,
                        bikes_available,
                        free_slots,
                        utilization_rate,
                        rolling_15m_utilization,
                        (bikes_available = 0) AS is_empty,
                        CASE
                            WHEN bikes_available = 0 THEN 'empty-and-critical'
                            ELSE 'critical'
                        END AS alert_level
                    FROM rolling_window
                    WHERE bikes_available <= %s
                      AND rolling_15m_utilization >= %s
                    """,
                    (
                        self.settings.critical_bikes_threshold,
                        self.settings.critical_utilization_threshold,
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO station_alerts (
                        station_key,
                        city,
                        station_id,
                        station_name,
                        snapshot_timestamp,
                        alert_type,
                        alert_message,
                        bikes_available,
                        free_slots,
                        utilization_rate
                    )
                    SELECT
                        station_key,
                        city,
                        station_id,
                        station_name,
                        snapshot_timestamp,
                        'empty' AS alert_type,
                        station_name || ' is empty' AS alert_message,
                        bikes_available,
                        free_slots,
                        utilization_rate
                    FROM latest_station_status
                    WHERE bikes_available = 0
                    ON CONFLICT (station_key, snapshot_timestamp, alert_type) DO NOTHING
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO station_alerts (
                        station_key,
                        city,
                        station_id,
                        station_name,
                        snapshot_timestamp,
                        alert_type,
                        alert_message,
                        bikes_available,
                        free_slots,
                        utilization_rate
                    )
                    SELECT
                        station_key,
                        city,
                        station_id,
                        station_name,
                        snapshot_timestamp,
                        'critical' AS alert_type,
                        station_name || ' is critically constrained' AS alert_message,
                        bikes_available,
                        free_slots,
                        utilization_rate
                    FROM critical_stations
                    ON CONFLICT (station_key, snapshot_timestamp, alert_type) DO NOTHING
                    """
                )

                cursor.execute("TRUNCATE station_status_stage")
            conn.commit()
        LOGGER.info("Refreshed analytics tables and alerts")

    def write_micro_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        self.bootstrap_schema()

        enriched_batch = (
            batch_df.withColumn("batch_id", F.lit(int(batch_id)))
            .select(*STAGE_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        try:
            self.archive_batch(enriched_batch)
            self.load_stage_table(enriched_batch)
            self.refresh_analytics()
        finally:
            enriched_batch.unpersist()
