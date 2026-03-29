package io.debezium.postgres2lake.domain.model;

public enum OutputPartitionStrategy {
    UNPARTITIONED, EVENT_TIME, PROCESSING_TIME, RECORD_FIELD
}
