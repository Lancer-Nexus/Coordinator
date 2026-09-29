-- Coordinator-owned NPC identity allocation and ownership fence.
-- Apply with the deployment migration runner before enabling npc_ownership_v1.

CREATE TABLE npc_id_allocation_batches (
    request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    requested_count SMALLINT UNSIGNED NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (request_id),
    CONSTRAINT ck_npc_id_allocation_count CHECK (requested_count BETWEEN 1 AND 256)
) ENGINE=InnoDB;

CREATE TABLE npc_ownership_leases (
    npc_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    ownership_version BIGINT NOT NULL,
    active_transfer_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    allocation_request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    allocation_ordinal SMALLINT UNSIGNED NOT NULL,
    created_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (npc_id),
    UNIQUE KEY uq_npc_allocation_ordinal (allocation_request_id, allocation_ordinal),
    KEY ix_npc_ownership_instance_system (instance_id, system_id),
    KEY ix_npc_ownership_active_transfer (active_transfer_id),
    CONSTRAINT ck_npc_ownership_version_positive CHECK (ownership_version > 0),
    CONSTRAINT fk_npc_allocation_batch FOREIGN KEY (allocation_request_id)
        REFERENCES npc_id_allocation_batches (request_id)
) ENGINE=InnoDB;

-- Durable handoff journal. Snapshot bytes stay in MySQL so a Coordinator restart
-- can resume a frozen transfer without making the filesystem registry authoritative.
CREATE TABLE npc_transfer_journal (
    transfer_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    idempotency_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    source_instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_instance_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_system_id VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    npc_ids MEDIUMBLOB NOT NULL,
    formation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    mission_runtime_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    expires_at_utc DATETIME(6) NOT NULL,
    state TINYINT UNSIGNED NOT NULL,
    snapshot MEDIUMBLOB NULL,
    snapshot_sha256 BINARY(32) NULL,
    created_at_utc DATETIME(6) NOT NULL,
    updated_at_utc DATETIME(6) NOT NULL,
    PRIMARY KEY (transfer_id),
    UNIQUE KEY uq_npc_transfer_idempotency (idempotency_key),
    KEY ix_npc_transfer_recovery (state, updated_at_utc),
    CONSTRAINT ck_npc_transfer_state CHECK (state IN (2, 4, 5, 6, 7, 22)),
    CONSTRAINT ck_npc_transfer_snapshot_hash CHECK
        ((snapshot IS NULL AND snapshot_sha256 IS NULL) OR (snapshot IS NOT NULL AND snapshot_sha256 IS NOT NULL))
) ENGINE=InnoDB;
