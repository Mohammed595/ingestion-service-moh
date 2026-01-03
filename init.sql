-- Initialize the events table and indexes
-- This script runs automatically when PostgreSQL starts

CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp BIGINT NOT NULL,
    received_at BIGINT NOT NULL,
    customer_id TEXT NOT NULL,
    restaurant_id TEXT NOT NULL,
    driver_id TEXT NOT NULL,
    location_lat DOUBLE PRECISION NOT NULL,
    location_lng DOUBLE PRECISION NOT NULL,
    platform_token TEXT NOT NULL,
    validation_status TEXT NOT NULL,
    validation_error TEXT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_order_id ON events(order_id);
CREATE INDEX IF NOT EXISTS idx_event_timestamp ON events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_received_at ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_validation_status ON events(validation_status);
CREATE INDEX IF NOT EXISTS idx_customer_id ON events(customer_id);
CREATE INDEX IF NOT EXISTS idx_restaurant_id ON events(restaurant_id);
CREATE INDEX IF NOT EXISTS idx_driver_id ON events(driver_id);
CREATE INDEX IF NOT EXISTS idx_platform_token ON events(platform_token);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_order_event_time ON events(order_id, event_type, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_customer_received ON events(customer_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_restaurant_time ON events(restaurant_id, event_timestamp);

-- Partial index for analytics queries (only valid events)
CREATE INDEX IF NOT EXISTS idx_valid_events_time ON events(event_timestamp) WHERE validation_status = 'valid';
