-- Retain the quote inputs that authorize a Gateway market reservation.
ALTER TABLE market_trade_reservations
    ADD COLUMN reputation_revision BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER total_price,
    ADD COLUMN reputation_price_factor_parts_per_million INT NOT NULL DEFAULT 1000000 AFTER reputation_revision,
    ADD CONSTRAINT chk_market_trade_reservation_reputation CHECK (
        reputation_price_factor_parts_per_million BETWEEN 900000 AND 1100000
    );
