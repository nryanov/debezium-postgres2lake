package io.debezium.postgres2lake.iceberg.infrastructure;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

import java.util.HashSet;

public class IcebergSchemaConverter implements SchemaConverter<Schema> {
    @Override
    public Schema extractSchema(EventRecord event) {
        var fields = AvroSchemaUtil.convert(event.valueSchema()).asNestedType().asStructType().fields();

        var pkFields = new HashSet<Integer>();
        var pkFieldNames = new HashSet<String>();
        event.keySchema().getFields().forEach(it -> pkFieldNames.add(it.name()));

        fields.forEach(it -> {
            if (pkFieldNames.contains(it.name())) {
                pkFields.add(it.fieldId());
            }
        });

        return new Schema(fields, pkFields);
    }
}
