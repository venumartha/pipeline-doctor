"""Fault scenarios with ground truth labels. Used by the UI, the fix logic, and evals."""

ROOT_CAUSES = [
    "slow_consumer",
    "schema_break",
    "downstream_timeout",
    "hot_partition",
    "poison_message",
    "ingest_data_loss",
]

ACTIONS = {
    "scale_consumers": "Add enricher consumer instances (rollback: scale back down)",
    "rollback_schema_and_replay_dlq": "Roll producer schema back one version, then replay DLQ (rollback: re-apply schema)",
    "pause_and_retry_with_backoff": "Pause the sink and retry with exponential backoff (rollback: resume normal retries)",
    "rebalance_partition_key": "Re-key the flooding device across partitions (rollback: restore old key)",
    "skip_poison_message": "Move the poison record to DLQ and commit past it (rollback: reprocess the offset)",
    "replay_from_source": "Alert owners and replay the gap from device buffers (rollback: none needed, idempotent)",
}

SCENARIOS = {
    "consumer_lag": {
        "label": "Consumer lag spike",
        "description": "Enricher slowed down 10x",
        "root_cause": "slow_consumer",
        "fix": "scale_consumers",
    },
    "bad_schema": {
        "label": "Bad message schema",
        "description": "Producer renamed a field",
        "root_cause": "schema_break",
        "fix": "rollback_schema_and_replay_dlq",
    },
    "sink_timeout": {
        "label": "Downstream API timeout",
        "description": "Downstream sink is timing out",
        "root_cause": "downstream_timeout",
        "fix": "pause_and_retry_with_backoff",
    },
    "hot_partition": {
        "label": "Hot partition",
        "description": "One device floods one partition key",
        "root_cause": "hot_partition",
        "fix": "rebalance_partition_key",
    },
    "poison_message": {
        "label": "Poison message",
        "description": "One malformed record keeps failing",
        "root_cause": "poison_message",
        "fix": "skip_poison_message",
    },
    "silent_drop": {
        "label": "Silent data loss",
        "description": "Ingest drops 30% with no errors",
        "root_cause": "ingest_data_loss",
        "fix": "replay_from_source",
    },
}

# Which scenario each action fixes, used when a human approves a proposal.
ACTION_FIXES = {v["fix"]: k for k, v in SCENARIOS.items()}
