-- Durable acknowledged simulation frames. Never prune while referenced by leases.
CREATE TABLE npc_checkpoint_writes (
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_payload MEDIUMBLOB NOT NULL,
    response_payload MEDIUMBLOB NULL,
    accepted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (request_id),
    KEY ix_npc_checkpoint_owner (instance_id, accepted, request_id)
) ENGINE=InnoDB;

ALTER TABLE npc_ownership_leases
    ADD COLUMN checkpoint_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN checkpoint_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD KEY ix_npc_current_checkpoint (checkpoint_id, npc_id),
    ADD CONSTRAINT ck_npc_checkpoint_revision CHECK (checkpoint_revision >= 0),
    ADD CONSTRAINT ck_npc_checkpoint_active CHECK
        (checkpoint_id IS NULL OR (checkpoint_revision > 0 AND retired_at_utc IS NULL)),
    ADD CONSTRAINT fk_npc_current_checkpoint FOREIGN KEY (checkpoint_id)
        REFERENCES npc_checkpoint_writes (request_id);
