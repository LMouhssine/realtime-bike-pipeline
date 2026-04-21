from __future__ import annotations

from pathlib import Path

import requests

from data_ingestion.producer import (
    CityBikesClient,
    NetworkTarget,
    normalize_station_payload,
    normalize_timestamp_value,
    read_network_targets_cache,
    select_target_networks,
    write_network_targets_cache,
)


def test_select_target_networks_filters_to_french_matches() -> None:
    networks = [
        {
            "id": "velib",
            "name": "Velib",
            "href": "/v2/networks/velib",
            "location": {"city": "Paris", "country": "FR"},
        },
        {
            "id": "velo-v",
            "name": "Velo'v",
            "href": "/v2/networks/velo-v",
            "location": {"city": "Lyon", "country": "FR"},
        },
        {
            "id": "bogus-paris",
            "name": "Bogus Paris",
            "href": "/v2/networks/bogus-paris",
            "location": {"city": "Paris", "country": "US"},
        },
    ]

    resolved = select_target_networks(networks, ("Paris", "Lyon"))

    assert resolved == [
        NetworkTarget(
            requested_city="Paris",
            city="Paris",
            network_id="velib",
            network_name="Velib",
            href="/v2/networks/velib",
        ),
        NetworkTarget(
            requested_city="Lyon",
            city="Lyon",
            network_id="velo-v",
            network_name="Velo'v",
            href="/v2/networks/velo-v",
        ),
    ]


def test_normalize_station_payload_preserves_required_contract() -> None:
    payload = normalize_station_payload(
        {
            "id": "123",
            "name": "Hotel de Ville",
            "latitude": 48.857,
            "longitude": 2.352,
            "free_bikes": 4,
            "empty_slots": 11,
            "timestamp": "2026-04-21T09:00:00Z",
        },
        network_id="velib",
        city="Paris",
        ingested_at="2026-04-21T09:00:01Z",
    )

    assert payload == {
        "station_id": "123",
        "station_name": "Hotel de Ville",
        "latitude": 48.857,
        "longitude": 2.352,
        "bikes_available": 4,
        "free_slots": 11,
        "timestamp": "2026-04-21T09:00:00Z",
        "network_id": "velib",
        "city": "Paris",
        "station_key": "velib:123",
        "ingested_at": "2026-04-21T09:00:01Z",
    }


def test_normalize_timestamp_value_strips_duplicate_utc_suffix() -> None:
    assert normalize_timestamp_value("2026-04-21T11:59:43.000998+00:00Z") == (
        "2026-04-21T11:59:43.000998+00:00"
    )


def test_network_targets_cache_round_trip(tmp_path: Path) -> None:
    cache_path = tmp_path / "resolved_networks.json"
    expected = [
        NetworkTarget(
            requested_city="Paris",
            city="Paris",
            network_id="velib",
            network_name="Velib",
            href="/v2/networks/velib",
        )
    ]

    write_network_targets_cache(cache_path, expected)

    assert read_network_targets_cache(cache_path) == expected


def test_resolve_network_targets_uses_cache_when_discovery_fails(tmp_path: Path) -> None:
    cache_path = tmp_path / "resolved_networks.json"
    cached_target = NetworkTarget(
        requested_city="Paris",
        city="Paris",
        network_id="velib",
        network_name="Velib",
        href="/v2/networks/velib",
    )
    write_network_targets_cache(cache_path, [cached_target])

    class DummySettings:
        citybikes_api_key = None
        citybikes_base_url = "https://api.citybik.es/v2"
        target_cities = ("Paris",)
        network_cache_path = cache_path

    client = CityBikesClient(DummySettings())
    client.fetch_network_catalog = lambda: (_ for _ in ()).throw(
        requests.RequestException("rate limit")
    )

    assert client.resolve_network_targets() == [cached_target]


def test_citybikes_client_disables_retry_after_handling_for_429() -> None:
    class DummySettings:
        citybikes_api_key = None
        citybikes_base_url = "https://api.citybik.es/v2"
        target_cities = ("Paris",)
        network_cache_path = Path("resolved_networks.json")

    client = CityBikesClient(DummySettings())
    retries = client.session.get_adapter("https://").max_retries

    assert retries.respect_retry_after_header is False
    assert 429 not in retries.status_forcelist
