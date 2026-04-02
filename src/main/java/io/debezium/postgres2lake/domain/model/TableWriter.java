package io.debezium.postgres2lake.domain.model;

import org.apache.avro.Schema;

public interface TableWriter {
    String partition();

    Schema schema();
}
