package io.debezium.postgres2lake.test.avro;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public final class AvroTestFixtures {

    public static final String TEST_NAMESPACE = "io.debezium.postgres2lake.test";
    public static final String RAW_DESTINATION = "db.schema.table";

    private AvroTestFixtures() {
    }

    public static Schema required(Schema.Type type) {
        return Schema.create(type);
    }

    public static Schema nullable(Schema inner) {
        return Schema.createUnion(Schema.create(Schema.Type.NULL), inner);
    }

    public static Schema record(String name, List<Schema.Field> fields) {
        return Schema.createRecord(name, null, TEST_NAMESPACE, false, fields);
    }

    public static Schema.Field field(String name, Schema schema) {
        return new Schema.Field(name, schema, null, null);
    }

    public static Schema decimalSchema(int precision, int scale) {
        var bytes = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(precision, scale).addToSchema(bytes);
        return bytes;
    }

    public static Schema timestampMillisLong(boolean adjustToUtc) {
        var schema = SchemaBuilder.builder().longBuilder().prop("adjust-to-utc", adjustToUtc).endLong();
        return LogicalTypes.timestampMillis().addToSchema(schema);
    }

    public static Schema timestampMicrosLong(boolean adjustToUtc) {
        var schema = SchemaBuilder.builder().longBuilder().prop("adjust-to-utc", adjustToUtc).endLong();
        return LogicalTypes.timestampMicros().addToSchema(schema);
    }

    public static GenericRecord emptyRecord(Schema schema) {
        return new GenericData.Record(schema);
    }

    public static EventRecord insertEvent(GenericRecord key, GenericRecord value) {
        return new EventRecord(Operation.INSERT, key, value, RAW_DESTINATION);
    }

    // value -- always generic record. Default field name -- just 'v'
    public static EventRecord singleFieldValueEvent(Schema fieldSchema) {
        var valueSchema = record("Val", List.of(field("v", fieldSchema)));
        var keySchema = record("Key", List.of());
        return insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema));
    }
}
