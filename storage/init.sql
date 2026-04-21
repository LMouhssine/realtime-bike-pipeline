CREATE TABLE IF NOT EXISTS station_status_stage (
    station_key TEXT NOT NULL,
    network_id TEXT NOT NULL,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    event_hour INTEGER NOT NULL,
    batch_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS station_status_facts (
    station_key TEXT NOT NULL,
    network_id TEXT NOT NULL,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    event_hour INTEGER NOT NULL,
    batch_id BIGINT NOT NULL,
    PRIMARY KEY (station_key, snapshot_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_station_status_facts_city_snapshot
    ON station_status_facts (city, snapshot_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_station_status_facts_zone_snapshot
    ON station_status_facts (zone_id, snapshot_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_station_status_facts_event_date
    ON station_status_facts (event_date, event_hour);

CREATE TABLE IF NOT EXISTS latest_station_status (
    station_key TEXT PRIMARY KEY,
    network_id TEXT NOT NULL,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    event_hour INTEGER NOT NULL,
    batch_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS top_utilization_stations (
    station_key TEXT PRIMARY KEY,
    network_id TEXT NOT NULL,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    utilization_rank INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS low_availability_zones (
    city TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    avg_bikes_available DOUBLE PRECISION NOT NULL,
    avg_free_slots DOUBLE PRECISION NOT NULL,
    avg_utilization_rate DOUBLE PRECISION NOT NULL,
    station_count INTEGER NOT NULL,
    shortage_flag BOOLEAN NOT NULL,
    PRIMARY KEY (city, zone_id)
);

CREATE TABLE IF NOT EXISTS daily_usage_peaks (
    city TEXT NOT NULL,
    usage_date DATE NOT NULL,
    usage_hour INTEGER NOT NULL,
    estimated_activity DOUBLE PRECISION NOT NULL,
    peak_rank INTEGER NOT NULL,
    PRIMARY KEY (city, usage_date, usage_hour)
);

CREATE TABLE IF NOT EXISTS geographic_imbalance (
    city TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    avg_bikes_available DOUBLE PRECISION NOT NULL,
    avg_capacity DOUBLE PRECISION NOT NULL,
    zone_availability_ratio DOUBLE PRECISION NOT NULL,
    city_availability_ratio DOUBLE PRECISION NOT NULL,
    imbalance_score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (city, zone_id)
);

CREATE TABLE IF NOT EXISTS critical_stations (
    station_key TEXT PRIMARY KEY,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    rolling_15m_utilization DOUBLE PRECISION NOT NULL,
    is_empty BOOLEAN NOT NULL,
    alert_level TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS station_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    station_key TEXT NOT NULL,
    city TEXT NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT NOT NULL,
    snapshot_timestamp TIMESTAMPTZ NOT NULL,
    alert_type TEXT NOT NULL,
    alert_message TEXT NOT NULL,
    bikes_available INTEGER NOT NULL,
    free_slots INTEGER NOT NULL,
    utilization_rate DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (station_key, snapshot_timestamp, alert_type)
);
