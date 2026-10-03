-- Retired NPC IDs remain reserved permanently and cannot enter new transfers.
ALTER TABLE npc_ownership_leases
    ADD COLUMN retired_at_utc DATETIME(6) NULL,
    ADD COLUMN retirement_reason TINYINT UNSIGNED NULL,
    ADD CONSTRAINT ck_npc_retirement_state CHECK
        ((retired_at_utc IS NULL AND retirement_reason IS NULL) OR
         (retired_at_utc IS NOT NULL AND retirement_reason IS NOT NULL AND retirement_reason IN (1, 2, 3) AND active_transfer_id IS NULL));

CREATE TABLE npc_retirement_batches (
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_payload MEDIUMBLOB NOT NULL,
    response_payload MEDIUMBLOB NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (request_id)
) ENGINE=InnoDB;
