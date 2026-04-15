package io.debezium.postgres2lake.core.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;

public class UnpartitionedEventPartitioner implements EventPartitioner {
    @Override
    public String resolvePartition(String rootPath, EventRecord record) {
        var destination = record.destination();
        return String.format("%s/%s/%s/%s", rootPath, destination.database(), destination.schema(), destination.table());
    }
}
