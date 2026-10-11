-- Adds report-pinned stock policy state and enforces capacity/price limits.
ALTER TABLE market_good_state
    ADD COLUMN reserved_sell_stock BIGINT NOT NULL DEFAULT 0 AFTER reserved_buy_stock,
    ADD COLUMN max_stock BIGINT NOT NULL DEFAULT 9223372036854775807 AFTER target_stock,
    ADD COLUMN min_unit_price BIGINT NOT NULL DEFAULT 0 AFTER max_stock,
    ADD COLUMN max_unit_price BIGINT NOT NULL DEFAULT 9223372036854775807 AFTER min_unit_price,
    ADD COLUMN pricing_policy_version INT NOT NULL DEFAULT 1 AFTER max_unit_price,
    ADD COLUMN policy_revision BIGINT NOT NULL DEFAULT 0 AFTER pricing_policy_version,
    ADD COLUMN market_faction_override_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER policy_revision,
    ADD CONSTRAINT chk_market_good_state_stock_capacity CHECK (
        reserved_sell_stock >= 0 AND reserved_sell_stock <= max_stock AND
        available_stock <= max_stock - reserved_sell_stock
    ),
    ADD CONSTRAINT chk_market_good_state_policy_bounds CHECK (
        max_stock > 0 AND min_unit_price >= 0 AND max_unit_price >= min_unit_price AND
        base_unit_price BETWEEN min_unit_price AND max_unit_price AND
        current_unit_price BETWEEN min_unit_price AND max_unit_price AND
        pricing_policy_version > 0 AND policy_revision >= 0
    );

CREATE TABLE market_stock_policy_installation (
    policy_slot TINYINT NOT NULL,
    catalog_seed_sha256 BINARY(32) NULL,
    policy_sha256 BINARY(32) NULL,
    policy_revision BIGINT NULL,
    policy_version VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    imported_utc DATETIME(6) NULL,
    entry_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (policy_slot),
    CONSTRAINT chk_market_stock_policy_slot CHECK (policy_slot = 1),
    CONSTRAINT chk_market_stock_policy_installation CHECK (
        (policy_sha256 IS NULL AND policy_revision IS NULL AND policy_version IS NULL AND imported_utc IS NULL AND entry_count = 0) OR
        (policy_sha256 IS NOT NULL AND policy_revision > 0 AND policy_version IS NOT NULL AND imported_utc IS NOT NULL AND entry_count > 0)
    )
) ENGINE=InnoDB;

INSERT INTO market_stock_policy_installation (policy_slot) VALUES (1);

CREATE TABLE market_stock_policy_entries (
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    policy_revision BIGINT NOT NULL,
    review_reason VARCHAR(512) NULL,
    PRIMARY KEY (market_id, good_id),
    CONSTRAINT fk_market_stock_policy_entry_state
        FOREIGN KEY (market_id, good_id) REFERENCES market_good_state (market_id, good_id),
    CONSTRAINT chk_market_stock_policy_entry_revision CHECK (policy_revision > 0)
) ENGINE=InnoDB;
