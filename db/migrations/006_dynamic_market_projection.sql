-- Shared read projection for location-specific dynamic market state.
-- market_id + good_id is authoritative; system_id only groups market locations.
CREATE TABLE market_system_revisions (
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_revision BIGINT NOT NULL,
    updated_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (system_id)
) ENGINE=InnoDB;

CREATE TABLE market_good_state (
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    base_unit_price BIGINT NOT NULL,
    current_unit_price BIGINT NOT NULL,
    available_stock BIGINT NOT NULL,
    target_stock BIGINT NOT NULL,
    buy_enabled BOOLEAN NOT NULL,
    sell_enabled BOOLEAN NOT NULL,
    market_revision BIGINT NOT NULL,
    PRIMARY KEY (market_id, good_id),
    KEY ix_market_good_state_system_page (system_id, market_id, good_id),
    CONSTRAINT fk_market_good_state_system_revision
        FOREIGN KEY (system_id) REFERENCES market_system_revisions (system_id),
    CONSTRAINT chk_market_good_state_nonnegative CHECK (
        base_unit_price >= 0 AND current_unit_price >= 0 AND
        available_stock >= 0 AND target_stock >= 0 AND market_revision > 0
    )
) ENGINE=InnoDB;
