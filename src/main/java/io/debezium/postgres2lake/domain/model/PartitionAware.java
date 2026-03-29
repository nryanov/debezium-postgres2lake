package io.debezium.postgres2lake.domain.model;

public interface PartitionAware {
    String partition();
}
