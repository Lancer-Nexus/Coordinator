-- Holds buy-side stock while Gateway durably commits the corresponding character trade.
ALTER TABLE market_good_state
    ADD COLUMN reserved_buy_stock BIGINT NOT NULL DEFAULT 0 AFTER available_stock,
    ADD CONSTRAINT chk_market_good_state_reserved_buy_stock
        CHECK (reserved_buy_stock >= 0 AND reserved_buy_stock <= available_stock);

CREATE TABLE market_trade_reservations (
    trade_id BINARY(16) NOT NULL,
    request_sha256 BINARY(32) NOT NULL,
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trade_side TINYINT NOT NULL,
    quantity BIGINT NOT NULL,
    quote_unit_price BIGINT NOT NULL,
    total_price BIGINT NOT NULL,
    state TINYINT NOT NULL,
    market_revision BIGINT NOT NULL,
    system_revision BIGINT NOT NULL,
    created_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (trade_id),
    KEY ix_market_trade_reservations_state_updated (state, updated_utc),
    CONSTRAINT fk_market_trade_reservations_good
        FOREIGN KEY (market_id, good_id) REFERENCES market_good_state (market_id, good_id),
    CONSTRAINT chk_market_trade_reservation_fields CHECK (
        trade_side IN (1, 2) AND quantity > 0 AND quote_unit_price >= 0 AND total_price >= 0 AND
        state IN (1, 2, 3) AND market_revision > 0 AND system_revision > 0
    )
) ENGINE=InnoDB;
