package io.debezium.postgres2lake.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SingleEventPartitioner implements EventPartitioner {
    @Override
    public String resolvePartition(String bucket, EventRecord record) {
        var destination = record.destination();
        return String.format("s3a://%s/%s/%s/%s", bucket, destination.database(), destination.schema(), destination.table());
    }
}
