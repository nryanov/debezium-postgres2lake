package io.debezium.postgres2lake.infrastructure.format.iceberg;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

public class IcebergSchemaConverter implements SchemaConverter<Schema> {
    @Override
    public Schema extractSchema(EventRecord event) {
        // todo: add information about PK
        return AvroSchemaUtil.toIceberg(event.valueSchema());
    }
}
