from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    if value is None:
        raise ValueError(f"Environment variable {name} resolved to None without a default")
    return value


def _get_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _get_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _get_path(name: str, default: Path) -> Path:
    raw_value = os.getenv(name, str(default))
    path = Path(raw_value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _parse_csv(value: str) -> tuple[str, ...]:
    items = [item.strip() for item in value.split(",")]
    return tuple(item for item in items if item)


@dataclass(frozen=True)
class Settings:
    citybikes_base_url: str
    citybikes_api_key: str | None
    target_cities: tuple[str, ...]
    poll_interval_seconds: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_client_id: str
    kafka_replication_factor: int
    kafka_starting_offsets: str
    network_cache_path: Path
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    spark_master_url: str
    spark_app_name: str
    spark_checkpoint_dir: str
    parquet_output_path: str
    low_availability_threshold: int
    critical_bikes_threshold: int
    critical_utilization_threshold: float
    top_station_limit: int
    streamlit_server_port: int

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def project_root(self) -> Path:
        return PROJECT_ROOT


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    citybikes_api_key = os.getenv("CITYBIKES_API_KEY")
    target_cities = _parse_csv(
        _get_env("TARGET_CITIES", "Paris,Lyon,Marseille,Toulouse,Bordeaux")
    )

    return Settings(
        citybikes_base_url=_get_env("CITYBIKES_BASE_URL", "https://api.citybik.es/v2"),
        citybikes_api_key=citybikes_api_key or None,
        target_cities=target_cities,
        poll_interval_seconds=_get_int("POLL_INTERVAL_SECONDS", 60),
        kafka_bootstrap_servers=_get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        kafka_topic=_get_env("KAFKA_TOPIC", "bike-stations"),
        kafka_client_id=_get_env("KAFKA_CLIENT_ID", "citybike-producer"),
        kafka_replication_factor=_get_int("KAFKA_REPLICATION_FACTOR", 1),
        kafka_starting_offsets=_get_env("KAFKA_STARTING_OFFSETS", "earliest").lower(),
        network_cache_path=_get_path(
            "CITYBIKES_NETWORK_CACHE_PATH",
            PROJECT_ROOT / "data" / "cache" / "resolved_networks.json",
        ),
        postgres_host=_get_env("POSTGRES_HOST", "localhost"),
        postgres_port=_get_int("POSTGRES_PORT", 5432),
        postgres_db=_get_env("POSTGRES_DB", "citybike"),
        postgres_user=_get_env("POSTGRES_USER", "citybike"),
        postgres_password=_get_env("POSTGRES_PASSWORD", "citybike"),
        spark_master_url=_get_env("SPARK_MASTER_URL", "spark://spark-master:7077"),
        spark_app_name=_get_env("SPARK_APP_NAME", "citybike-stream-analytics"),
        spark_checkpoint_dir=_get_env(
            "SPARK_CHECKPOINT_DIR", "/opt/project/data/checkpoints/citybike_stream"
        ),
        parquet_output_path=_get_env(
            "PARQUET_OUTPUT_PATH", "/opt/project/data/parquet/station_status"
        ),
        low_availability_threshold=_get_int("LOW_AVAILABILITY_THRESHOLD", 2),
        critical_bikes_threshold=_get_int("CRITICAL_BIKES_THRESHOLD", 2),
        critical_utilization_threshold=_get_float("CRITICAL_UTILIZATION_THRESHOLD", 0.85),
        top_station_limit=_get_int("TOP_STATION_LIMIT", 10),
        streamlit_server_port=_get_int("STREAMLIT_SERVER_PORT", 8501),
    )
