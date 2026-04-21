from __future__ import annotations

import json
import logging
import re
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import Settings, load_settings


LOGGER = logging.getLogger("citybike.producer")
SUPPORTED_COUNTRY_CODES = {"FR", "FRA"}


@dataclass(frozen=True)
class NetworkTarget:
    requested_city: str
    city: str
    network_id: str
    network_name: str
    href: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def normalize_label(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_only.lower().strip().split())


def normalize_timestamp_value(value: Any) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    if not normalized:
        return None

    # Some CityBikes payloads include both an explicit UTC offset and a trailing Z.
    return re.sub(r"([+-]\d{2}:?\d{2})Z$", r"\1", normalized)


def read_network_targets_cache(cache_path: Path) -> list[NetworkTarget]:
    if not cache_path.exists():
        return []

    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    return [NetworkTarget(**item) for item in payload]


def write_network_targets_cache(cache_path: Path, targets: list[NetworkTarget]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps([asdict(target) for target in targets], ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def is_rate_limited(exc: requests.RequestException) -> bool:
    response = getattr(exc, "response", None)
    return response is not None and response.status_code == 429


def score_city_match(requested_city: str, actual_city: str) -> tuple[int, int]:
    requested = normalize_label(requested_city)
    actual = normalize_label(actual_city)
    if requested == actual:
        return (0, len(actual))
    if requested in actual or actual in requested:
        return (1, len(actual))
    return (99, len(actual))


def select_target_networks(
    networks: list[dict[str, Any]], requested_cities: tuple[str, ...]
) -> list[NetworkTarget]:
    resolved_networks: list[NetworkTarget] = []

    for requested_city in requested_cities:
        eligible: list[dict[str, Any]] = []
        for network in networks:
            location = network.get("location", {})
            country_code = str(location.get("country", "")).upper()
            actual_city = location.get("city", "")

            if country_code not in SUPPORTED_COUNTRY_CODES:
                continue
            if score_city_match(requested_city, actual_city)[0] >= 99:
                continue
            eligible.append(network)

        if not eligible:
            LOGGER.warning("No CityBikes network matched configured city '%s'", requested_city)
            continue

        eligible.sort(
            key=lambda candidate: (
                score_city_match(requested_city, candidate.get("location", {}).get("city", "")),
                normalize_label(candidate.get("name", "")),
            )
        )
        selected = eligible[0]
        selected_city = selected.get("location", {}).get("city", requested_city)
        if len(eligible) > 1:
            LOGGER.warning(
                "Multiple networks matched city '%s'; selected '%s' (%s)",
                requested_city,
                selected.get("name", selected.get("id", "unknown")),
                selected.get("id", "unknown"),
            )

        resolved_networks.append(
            NetworkTarget(
                requested_city=requested_city,
                city=selected_city,
                network_id=selected["id"],
                network_name=selected.get("name", selected["id"]),
                href=selected.get("href", f"/networks/{selected['id']}"),
            )
        )

    if not resolved_networks:
        raise RuntimeError(
            "Unable to resolve any French CityBikes networks for the configured TARGET_CITIES."
        )

    return resolved_networks


def normalize_station_payload(
    station: dict[str, Any],
    *,
    network_id: str,
    city: str,
    ingested_at: str,
) -> dict[str, Any]:
    station_id = station.get("id") or station.get("extra", {}).get("uid")
    timestamp = normalize_timestamp_value(station.get("timestamp")) or ingested_at

    return {
        "station_id": str(station_id) if station_id is not None else None,
        "station_name": station.get("name") or station.get("extra", {}).get("address"),
        "latitude": station.get("latitude"),
        "longitude": station.get("longitude"),
        "bikes_available": station.get("free_bikes"),
        "free_slots": station.get("empty_slots"),
        "timestamp": timestamp,
        "network_id": network_id,
        "city": city,
        "station_key": f"{network_id}:{station_id}" if station_id is not None else None,
        "ingested_at": normalize_timestamp_value(ingested_at),
    }


class CityBikesClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()

        retries = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @property
    def cache_path(self) -> Path:
        return self.settings.network_cache_path

    @property
    def headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.settings.citybikes_api_key:
            headers["x-api-key"] = self.settings.citybikes_api_key
        return headers

    def fetch_network_catalog(self) -> list[dict[str, Any]]:
        url = f"{self.settings.citybikes_base_url.rstrip('/')}/networks"
        response = self.session.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        return payload.get("networks", [])

    def resolve_network_targets(self) -> list[NetworkTarget]:
        try:
            networks = self.fetch_network_catalog()
            targets = select_target_networks(networks, self.settings.target_cities)
            write_network_targets_cache(self.cache_path, targets)
            LOGGER.info(
                "Resolved %s target network(s): %s",
                len(targets),
                ", ".join(f"{target.city}={target.network_id}" for target in targets),
            )
            LOGGER.info("Cached resolved network targets at %s", self.cache_path)
            return targets
        except (requests.RequestException, RuntimeError) as exc:
            cached_targets = read_network_targets_cache(self.cache_path)
            if cached_targets:
                LOGGER.warning(
                    "CityBikes network discovery failed%s. Falling back to cached targets from %s",
                    " with HTTP 429 rate limiting" if isinstance(exc, requests.RequestException) and is_rate_limited(exc) else f" ({exc})",
                    self.cache_path,
                )
                return cached_targets
            raise

    def fetch_network_snapshot(self, network: NetworkTarget) -> list[dict[str, Any]]:
        if network.href.startswith("http://") or network.href.startswith("https://"):
            url = network.href
        elif network.href.startswith("/v2/"):
            parsed_base = urlparse(self.settings.citybikes_base_url)
            url = f"{parsed_base.scheme}://{parsed_base.netloc}{network.href}"
        else:
            url = f"{self.settings.citybikes_base_url.rstrip('/')}/{network.href.lstrip('/')}"

        response = self.session.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        network_payload = payload.get("network", {})
        return network_payload.get("stations", [])


def build_kafka_producer(settings: Settings):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        retries=10,
        linger_ms=250,
        max_block_ms=30000,
        value_serializer=lambda payload: json.dumps(payload, default=str).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )


def ensure_topic_exists(settings: Settings) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-admin",
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            "Kafka broker is not reachable. Start docker compose services before the producer."
        ) from exc

    try:
        admin.create_topics(
            [
                NewTopic(
                    name=settings.kafka_topic,
                    num_partitions=1,
                    replication_factor=settings.kafka_replication_factor,
                )
            ]
        )
        LOGGER.info("Created Kafka topic '%s'", settings.kafka_topic)
    except TopicAlreadyExistsError:
        LOGGER.info("Kafka topic '%s' already exists", settings.kafka_topic)
    finally:
        admin.close()


def run_producer() -> None:
    configure_logging()
    settings = load_settings()
    client = CityBikesClient(settings)

    ensure_topic_exists(settings)
    targets = client.resolve_network_targets()
    producer = build_kafka_producer(settings)

    LOGGER.info(
        "Starting CityBikes producer with poll interval=%ss for cities=%s",
        settings.poll_interval_seconds,
        ", ".join(settings.target_cities),
    )

    while True:
        cycle_started = time.monotonic()
        ingested_at = (
            datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        produced_messages = 0

        for target in targets:
            try:
                stations = client.fetch_network_snapshot(target)
                for station in stations:
                    payload = normalize_station_payload(
                        station,
                        network_id=target.network_id,
                        city=target.city,
                        ingested_at=ingested_at,
                    )
                    if payload["station_key"] is None:
                        continue
                    producer.send(
                        settings.kafka_topic,
                        key=payload["station_key"],
                        value=payload,
                    )
                    produced_messages += 1
                LOGGER.info(
                    "Published %s station snapshot(s) for %s (%s)",
                    len(stations),
                    target.city,
                    target.network_id,
                )
            except requests.RequestException as exc:
                if is_rate_limited(exc):
                    LOGGER.warning(
                        "CityBikes rate limit hit for %s (%s). No new snapshot published this cycle.",
                        target.city,
                        target.network_id,
                    )
                else:
                    LOGGER.exception(
                        "CityBikes request failed for %s (%s)",
                        target.city,
                        target.network_id,
                    )
            except Exception:
                LOGGER.exception(
                    "Unexpected producer failure for %s (%s)", target.city, target.network_id
                )

        producer.flush()
        elapsed = time.monotonic() - cycle_started
        sleep_seconds = max(settings.poll_interval_seconds - elapsed, 0)
        LOGGER.info(
            "Cycle complete with %s total message(s); sleeping %.2fs",
            produced_messages,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    run_producer()
