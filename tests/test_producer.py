from __future__ import annotations

from data_ingestion.producer import NetworkTarget, normalize_station_payload, select_target_networks


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
