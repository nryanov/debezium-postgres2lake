package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import org.apache.avro.Schema;

public interface SchemaEvolutionHandler {
    boolean supportSchemaEvolution();

    boolean isSchemaChanged(Schema current, Schema next);

    AvroSchemaChanges resolveDiff(Schema current, Schema next);
}
