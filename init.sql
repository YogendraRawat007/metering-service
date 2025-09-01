
CREATE DATABASE IF NOT EXISTS metering_db;

\c metering_db;

CREATE TABLE IF NOT EXISTS meters (
    meter_id SERIAL PRIMARY KEY,
    external_id VARCHAR(255) NOT NULL UNIQUE,
    cdf_service VARCHAR(100) NOT NULL,
    metric_key VARCHAR(100) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    value_property VARCHAR(255) NOT NULL,
    aggregation VARCHAR(50) NOT NULL DEFAULT 'SUM',
    group_by JSONB DEFAULT '{}',
    window_size VARCHAR(20) NOT NULL DEFAULT 'DAY',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS aggregated_usage (
    id SERIAL PRIMARY KEY,
    time TIMESTAMP NOT NULL,
    project_id VARCHAR(255) NOT NULL,
    cdf_service VARCHAR(100) NOT NULL,
    metric_key VARCHAR(100) NOT NULL,
    group_by_dims JSONB DEFAULT '{}',
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    value_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_aggregated_usage_unique 
ON aggregated_usage (time, project_id, cdf_service, metric_key, (group_by_dims::text));

CREATE INDEX IF NOT EXISTS idx_meters_lookup 
ON meters (cdf_service, event_type);

INSERT INTO meters (external_id, cdf_service, metric_key, event_type, value_property, aggregation, group_by, window_size) 
VALUES 
    ('meter_1', 'Timeseries', 'requests', 'request', '$.data.total_requests', 'SUM', '{}', 'DAY'),
    ('meter_2', 'Atlas', 'prompts', 'atlas_event', '$.data.prompts_used', 'SUM', '{}', 'HOUR'),
    ('meter_3', 'Atlas', 'coins', 'atlas_event', '$.data.coins_consumed', 'SUM', '{}', 'HOUR'),
    ('meter_4', 'Data Modelling', 'requests', 'request', '$.data.total_requests', 'SUM', '{}', 'DAY')
ON CONFLICT (external_id) DO NOTHING;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_meters_updated_at ON meters;
CREATE TRIGGER update_meters_updated_at
    BEFORE UPDATE ON meters
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_aggregated_usage_updated_at ON aggregated_usage;
CREATE TRIGGER update_aggregated_usage_updated_at
    BEFORE UPDATE ON aggregated_usage
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
