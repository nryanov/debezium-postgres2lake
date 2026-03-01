package io.debezium.postgres2lake.engine.avro;

import io.debezium.engine.ChangeEvent;
import io.debezium.postgres2lake.domain.EventRecord;
import io.debezium.postgres2lake.domain.Operation;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

@ApplicationScoped
public class UnwrappedGenericRecordSerde {
    private static final String OPERATION_FIELD_NAME = "op";
    private static final String AFTER_FIELD_NAME = "after";
    private static final String BEFORE_FIELD_NAME = "before";
    private static final String SOURCE_FIELD_NAME = "source";
    private static final String SOURCE_LSN_FIELD_NAME = "lsn";
    private static final String SOURCE_TS_MS_FIELD_NAME = "ts_ms";

    private static final String UNWRAPPED_OPERATION_FIELD_NAME = "__operation";
    private static final String UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME = "__idempotency_key";
    private static final String UNWRAPPED_EVENT_TIME_FIELD_NAME = "__event_time";

    private final GenericRecordSerde serde;

    public UnwrappedGenericRecordSerde(GenericRecordSerde serde) {
        this.serde = serde;
    }

    public EventRecord deserialize(ChangeEvent<Object, Object> event) {
        var keyPart = (byte[]) event.key();
        var valuePart = (byte[]) event.value();

        var key = serde.deserialize(keyPart);
        var value = unwrap(serde.deserialize(valuePart));

        return new EventRecord(key, value, event.destination());
    }

    private GenericRecord unwrap(GenericRecord payload) {
        var op = ((Utf8) payload.get(OPERATION_FIELD_NAME)).toString();
        var operation = switch (op) {
            case "c", "r" -> Operation.INSERT;
            case "u" -> Operation.UPDATE;
            case "d" -> Operation.DELETE;
            case null, default -> throw new IllegalArgumentException("unknown operation: " + op);
        };

        var sourcePart = (GenericRecord) payload.get(SOURCE_FIELD_NAME);
        var lsn = (Long) sourcePart.get(SOURCE_LSN_FIELD_NAME);
        var timestampMillis = (Long) sourcePart.get(SOURCE_TS_MS_FIELD_NAME);

        var unwrappedValues = switch (operation) {
            case INSERT, UPDATE -> (GenericRecord) payload.get(AFTER_FIELD_NAME);
            case DELETE -> (GenericRecord) payload.get(BEFORE_FIELD_NAME);
        };

        var unwrappedSchema = getOrCreateUnwrappedSchema(payload, unwrappedValues);
        var unwrappedRecord = new GenericData.Record(unwrappedSchema);

        for (var field : unwrappedValues.getSchema().getFields()) {
            unwrappedRecord.put(field.name(), unwrappedValues.get(field.name()));
        }

        unwrappedRecord.put(UNWRAPPED_OPERATION_FIELD_NAME, operation.name());
        unwrappedRecord.put(UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME, lsn);
        unwrappedRecord.put(UNWRAPPED_EVENT_TIME_FIELD_NAME, timestampMillis);

        return unwrappedRecord;
    }

    private Schema getOrCreateUnwrappedSchema(GenericRecord payload, GenericRecord values) {
        var initialSchema = payload.getSchema();
        var valuesSchema = values.getSchema();

        var schemaBuilder = SchemaBuilder
                .builder()
                .record(initialSchema.getName())
                .namespace(initialSchema.getNamespace())
                .fields();

        schemaBuilder.requiredString(UNWRAPPED_OPERATION_FIELD_NAME);
        schemaBuilder.requiredLong(UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME);
        schemaBuilder.requiredLong(UNWRAPPED_EVENT_TIME_FIELD_NAME);

        for (var field : valuesSchema.getFields()) {
            var fieldBuilder = schemaBuilder
                    .name(field.name())
                    .doc(field.doc())
                    .type(field.schema());

            if (field.hasDefaultValue() && field.defaultVal() != JsonProperties.NULL_VALUE) {
                fieldBuilder.withDefault(field.defaultVal());
            } else if (field.defaultVal() == JsonProperties.NULL_VALUE) {
                fieldBuilder.withDefault(null);
            } else {
                fieldBuilder.noDefault();
            }
        }

        return schemaBuilder.endRecord();
    }
}
