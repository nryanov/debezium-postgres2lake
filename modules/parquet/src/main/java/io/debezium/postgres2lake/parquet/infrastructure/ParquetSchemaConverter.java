package io.debezium.postgres2lake.parquet.infrastructure;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.Schema;

public class ParquetSchemaConverter implements SchemaConverter<Schema> {
    @Override
    public Schema extractSchema(EventRecord event) {
        return event.valueSchema();
    }
}
