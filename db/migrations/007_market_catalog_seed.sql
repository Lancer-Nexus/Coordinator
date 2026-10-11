-- Immutable seed source for the full goods catalog and static per-location offers.
-- Live stock and prices remain in market_good_state and are never inferred from INI Max.
CREATE TABLE market_catalog_seed (
    seed_slot TINYINT NOT NULL,
    seed_sha256 BINARY(32) NULL,
    imported_utc DATETIME(6) NULL,
    good_count INT NOT NULL DEFAULT 0,
    system_count INT NOT NULL DEFAULT 0,
    market_count INT NOT NULL DEFAULT 0,
    offer_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (seed_slot),
    CONSTRAINT chk_market_catalog_seed_slot CHECK (seed_slot = 1)
) ENGINE=InnoDB;

INSERT INTO market_catalog_seed (seed_slot) VALUES (1);

CREATE TABLE market_catalog_goods (
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    category VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    base_unit_price BIGINT NULL,
    PRIMARY KEY (good_id),
    CONSTRAINT chk_market_catalog_good_price CHECK (base_unit_price IS NULL OR base_unit_price >= 0)
) ENGINE=InnoDB;

CREATE TABLE market_locations (
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    local_faction_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (market_id),
    KEY ix_market_locations_system (system_id, market_id),
    CONSTRAINT fk_market_locations_system_revision
        FOREIGN KEY (system_id) REFERENCES market_system_revisions (system_id)
) ENGINE=InnoDB;

CREATE TABLE market_static_offers (
    market_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    good_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    baseline_unit_price BIGINT NOT NULL,
    buy_enabled BOOLEAN NOT NULL,
    rank_requirement INT NOT NULL,
    min_amount INT NOT NULL,
    max_amount INT NOT NULL,
    reputation_threshold DECIMAL(10, 6) NOT NULL,
    source_file VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (market_id, good_id),
    CONSTRAINT fk_market_static_offers_location
        FOREIGN KEY (market_id) REFERENCES market_locations (market_id),
    CONSTRAINT fk_market_static_offers_good
        FOREIGN KEY (good_id) REFERENCES market_catalog_goods (good_id),
    CONSTRAINT chk_market_static_offer_values CHECK (
        baseline_unit_price >= 0 AND rank_requirement >= 0 AND min_amount >= 0 AND max_amount >= 0
    )
) ENGINE=InnoDB;
