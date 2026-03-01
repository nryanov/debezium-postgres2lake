package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventRecord;

public interface EventPartitioner {
    String resolvePartition(String bucket, EventRecord record);
}
