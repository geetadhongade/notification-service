-- One row per event this service has already handled, so a redelivered event is skipped (no second SMS)
CREATE TABLE processed_event (
    -- The eventId from shop-api. PRIMARY KEY: the same ID can't be stored twice
    event_id     UUID PRIMARY KEY,
    -- Not needed for the duplicate check, but makes the table easy to read
    event_type   VARCHAR(50) NOT NULL,
    order_id     BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
