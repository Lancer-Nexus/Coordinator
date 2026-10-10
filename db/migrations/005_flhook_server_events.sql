-- Durable event acceptance and deduplication for authenticated game instances.
CREATE TABLE flhook_server_events (
    instance_id VARCHAR(128) NOT NULL,
    producer_epoch CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sequence_number BIGINT NOT NULL,
    occurred_utc DATETIME(6) NOT NULL,
    received_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    payload_sha256 BINARY(32) NOT NULL,
    event_payload MEDIUMBLOB NOT NULL,
    PRIMARY KEY (instance_id, producer_epoch, sequence_number),
    KEY ix_flhook_server_events_received (received_utc)
) ENGINE=InnoDB;
