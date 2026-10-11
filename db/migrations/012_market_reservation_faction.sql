-- Preserve which market faction was used to derive a quote-bound reservation price.
ALTER TABLE market_trade_reservations
    ADD COLUMN market_faction_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin
        NOT NULL DEFAULT 'unbound' AFTER reputation_price_factor_parts_per_million,
    ADD CONSTRAINT chk_market_trade_reservation_faction CHECK (
        market_faction_id = 'unbound' OR market_faction_id REGEXP '^[a-z0-9_.-]+$'
    );
