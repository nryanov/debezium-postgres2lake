package io.debezium.postgres2lake.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public final class EventRecordTestSupport {

    static final String RAW_DESTINATION = "db.schema.table";

    private static final Schema NULLABLE_STRING = Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING));

    private EventRecordTestSupport() {
    }

    public static EventRecord recordWithEventTime(long eventTimeEpochMillis) {
        var valueSchema = Schema.createRecord("value", null, null, false, List.of(
                        new Schema.Field(EventRecord.UNWRAPPED_OPERATION_FIELD_NAME, Schema.create(Schema.Type.STRING), null, null),
                        new Schema.Field(EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME, Schema.create(Schema.Type.LONG), null, null),
                        new Schema.Field(EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME, Schema.create(Schema.Type.LONG), null, null)
                )
        );

        var value = new GenericData.Record(valueSchema);

        value.put(EventRecord.UNWRAPPED_OPERATION_FIELD_NAME, "INSERT");
        value.put(EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME, 1L);
        value.put(EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME, eventTimeEpochMillis);

        return new EventRecord(Operation.INSERT, emptyKey(), value, RAW_DESTINATION);
    }

    public static EventRecord recordWithPartitionField(String fieldName, Schema fieldSchema, Object fieldValue) {
        var valueSchema = Schema.createRecord("value", null, null, false, List.of(
                new Schema.Field(fieldName, fieldSchema, null, null)));

        var value = new GenericData.Record(valueSchema);
        value.put(fieldName, fieldValue);

        return new EventRecord(Operation.INSERT, emptyKey(), value, RAW_DESTINATION);
    }

    public static EventRecord recordWithOnlyOtherField() {
        var valueSchema = Schema.createRecord("value", null, null, false, List.of(
                new Schema.Field("other", Schema.create(Schema.Type.STRING), null, null))
        );

        var value = new GenericData.Record(valueSchema);
        value.put("other", "ignored");

        return new EventRecord(Operation.INSERT, emptyKey(), value, RAW_DESTINATION);
    }

    static Schema nullableString() {
        return NULLABLE_STRING;
    }

    private static GenericRecord emptyKey() {
        return new GenericData.Record(Schema.createRecord("key", null, null, false, List.of()));
    }
}
