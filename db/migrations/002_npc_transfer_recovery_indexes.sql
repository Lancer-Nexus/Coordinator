-- Keep the per-instance committed-transfer recovery scan bounded by its page size.
ALTER TABLE npc_transfer_journal
    ADD KEY ix_npc_transfer_target_recovery (target_instance_id, state, transfer_id),
    ADD KEY ix_npc_transfer_source_recovery (source_instance_id, state, transfer_id);
