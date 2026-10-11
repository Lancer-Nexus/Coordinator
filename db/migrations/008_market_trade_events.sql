-- Idempotency journal for confirmed live market effects.
CREATE TABLE market_trade_events (
    event_id BINARY(16) NOT NULL,
    instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    producer_epoch BINARY(16) NOT NULL,
    sequence_number BIGINT NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_sha256 BINARY(32) NOT NULL,
    resulting_market_revision BIGINT NOT NULL,
    applied_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (event_id),
    UNIQUE KEY uq_market_trade_stream (instance_id, producer_epoch, sequence_number),
    KEY ix_market_trade_market_time (market_id, good_id, applied_utc),
    CONSTRAINT chk_market_trade_sequence CHECK (sequence_number > 0),
    CONSTRAINT chk_market_trade_revision CHECK (resulting_market_revision > 0)
) ENGINE=InnoDB;
