package io.debezium.postgres2lake.infrastructure.debezium.avro;

import io.debezium.engine.ChangeEvent;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class UnwrappedEventRecordDeserializerTest {

    private static final String DESTINATION = "db.schema.table";
    private static final long DEFAULT_LSN = 42L;
    private static final long DEFAULT_TS_MS = 1705312200000L;
    private static final String FIELD_NAME = "f";

    private record StubDeserializer(GenericRecord key, GenericRecord value) implements GenericRecordDeserializer {

        @Override
        public GenericRecord deserializeKey(String topic, byte[] data) {
            return key;
        }

        @Override
        public GenericRecord deserializeValue(String topic, byte[] data) {
            return value;
        }
    }

    private ChangeEvent<Object, Object> changeEvent() {
        return new ChangeEvent<>() {
            @Override
            public Object key() { return new byte[0]; }

            @Override
            public Object value() { return new byte[0]; }

            @Override
            public String destination() { return DESTINATION; }

            @Override
            public Integer partition() {
                return 0;
            }
        };
    }

    private GenericRecord emptyKey() {
        var schema = Schema.createRecord("Key", null, "io.debezium.connector.postgresql", false,
                List.of(new Schema.Field("id", Schema.create(Schema.Type.LONG))));
        var record = new GenericData.Record(schema);
        record.put("id", 1L);
        return record;
    }

    private Schema sourceSchema() {
        return Schema.createRecord("Source", null, "io.debezium.connector.postgresql", false, List.of(
                new Schema.Field("lsn", Schema.create(Schema.Type.LONG)),
                new Schema.Field("ts_ms", Schema.create(Schema.Type.LONG))));
    }

    private GenericRecord sourceRecord(Schema sourceSchema, long lsn, long tsMs) {
        var record = new GenericData.Record(sourceSchema);
        record.put("lsn", lsn);
        record.put("ts_ms", tsMs);
        return record;
    }

    private GenericRecord envelope(String op, Schema innerSchema, GenericRecord before, GenericRecord after) {
        var srcSchema = sourceSchema();
        var envelopeSchema = Schema.createRecord("Envelope", null, "io.debezium.connector.postgresql", false, List.of(
                new Schema.Field("op", Schema.create(Schema.Type.STRING)),
                new Schema.Field("before", innerSchema),
                new Schema.Field("after", innerSchema),
                new Schema.Field("source", srcSchema)));

        var record = new GenericData.Record(envelopeSchema);
        record.put("op", new Utf8(op));
        record.put("before", before);
        record.put("after", after);
        record.put("source", sourceRecord(srcSchema, DEFAULT_LSN, DEFAULT_TS_MS));
        return record;
    }

    private GenericRecord envelope(String op, GenericRecord inner) {
        return envelope(op, inner.getSchema(), inner, inner);
    }

    private Schema innerSchema(String fieldName, Schema fieldSchema) {
        return Schema.createRecord("Value", null, "io.debezium.connector.postgresql", false,
                List.of(new Schema.Field(fieldName, fieldSchema)));
    }

    private GenericRecord innerRecord(String fieldName, Schema fieldSchema, Object value) {
        var schema = innerSchema(fieldName, fieldSchema);
        var record = new GenericData.Record(schema);
        record.put(fieldName, value);
        return record;
    }

    private Schema innerSchema(List<Schema.Field> fields) {
        return Schema.createRecord("Value", null, "io.debezium.connector.postgresql", false, fields);
    }

    private EventRecord deserialize(GenericRecord envelopeRecord) {
        var serde = new StubDeserializer(emptyKey(), envelopeRecord);
        var deserializer = new UnwrappedEventRecordDeserializer(serde);
        return deserializer.deserialize(changeEvent());
    }

    private EventRecord deserializeInsert(String fieldName, Schema fieldSchema, Object inputValue) {
        var inner = innerRecord(fieldName, fieldSchema, inputValue);
        return deserialize(envelope("c", inner));
    }

    private static Schema connectSchema(Schema.Type baseType, String connectName) {
        var schema = Schema.create(baseType);
        schema.addProp("connect.name", connectName);
        return schema;
    }

    @Test
    void createOpResolvesToInsert() {
        var result = deserializeInsert(FIELD_NAME, Schema.create(Schema.Type.INT), 1);
        assertEquals(Operation.INSERT, result.operation());
    }

    @Test
    void readOpResolvesToInsert() {
        var inner = innerRecord(FIELD_NAME, Schema.create(Schema.Type.INT), 1);
        var result = deserialize(envelope("r", inner));
        assertEquals(Operation.INSERT, result.operation());
    }

    @Test
    void updateOpResolvesToUpdate() {
        var inner = innerRecord(FIELD_NAME, Schema.create(Schema.Type.INT), 1);
        var result = deserialize(envelope("u", inner));
        assertEquals(Operation.UPDATE, result.operation());
    }

    @Test
    void deleteOpResolvesToDeleteAndReadsFromBefore() {
        var schema = innerSchema(FIELD_NAME, Schema.create(Schema.Type.INT));

        var before = new GenericData.Record(schema);
        before.put(FIELD_NAME, 100);

        var after = new GenericData.Record(schema);
        after.put(FIELD_NAME, 200);

        var result = deserialize(envelope("d", schema, before, after));

        assertEquals(Operation.DELETE, result.operation());
        assertEquals(100, result.value().get(FIELD_NAME));
    }

    @Test
    void insertReadsFromAfter() {
        var schema = innerSchema(FIELD_NAME, Schema.create(Schema.Type.INT));

        var before = new GenericData.Record(schema);
        before.put(FIELD_NAME, 100);

        var after = new GenericData.Record(schema);
        after.put(FIELD_NAME, 200);

        var result = deserialize(envelope("c", schema, before, after));

        assertEquals(Operation.INSERT, result.operation());
        assertEquals(200, result.value().get(FIELD_NAME));
    }

    // TODO: make domain error
    @Test
    void unknownOpThrows() {
        var inner = innerRecord(FIELD_NAME, Schema.create(Schema.Type.INT), 1);
        var env = envelope("x", inner);
        assertThrows(IllegalArgumentException.class, () -> deserialize(env));
    }

    @Test
    void systemFieldsArePopulated() {
        var result = deserializeInsert(FIELD_NAME, Schema.create(Schema.Type.INT), 1);

        assertEquals("INSERT", result.value().get(EventRecord.UNWRAPPED_OPERATION_FIELD_NAME).toString());
        assertEquals(DEFAULT_LSN, result.value().get(EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME));
        assertEquals(DEFAULT_TS_MS, result.value().get(EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME));
    }

    @Test
    void destinationIsPropagated() {
        var result = deserializeInsert(FIELD_NAME, Schema.create(Schema.Type.INT), 1);

        assertEquals(DESTINATION, result.rawDestination());
        assertEquals("db", result.destination().database());
        assertEquals("schema", result.destination().schema());
        assertEquals("table", result.destination().table());
    }

    @Test
    void bits() {
        var inputSchema = connectSchema(Schema.Type.BYTES, "io.debezium.data.Bits");
        var inputValue = ByteBuffer.wrap(new byte[]{1, 0, 1});

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        assertEquals("io.debezium.data.Bits", result.valueSchema().getField(FIELD_NAME).schema().getProp("initial-type"));
    }

    @Test
    void zonedTimestamp() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.ZonedTimestamp");
        var inputValue = new Utf8("2024-01-15T10:30:00Z");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        var odt = OffsetDateTime.parse("2024-01-15T10:30:00Z");
        long expectedMicros = odt.toInstant().getEpochSecond() * 1_000_000L + odt.toInstant().getNano() / 1_000L;
        assertEquals(expectedMicros, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-micros", fieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.ZonedTimestamp", fieldSchema.getProp("initial-type"));
    }

    @Test
    void zonedTime() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.ZonedTime");
        var inputValue = new Utf8("10:30:00+00:00");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        int expectedMillis = (int) OffsetTime.parse("10:30:00+00:00").getLong(ChronoField.MILLI_OF_DAY);
        assertEquals(expectedMillis, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-millis", fieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.ZonedTime", fieldSchema.getProp("initial-type"));
    }

    @Test
    void isoDate() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.IsoDate");
        var inputValue = new Utf8("2024-01-15+00:00");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        int expectedDays = (int) LocalDate.of(2024, 1, 15).toEpochDay();
        assertEquals(expectedDays, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("date", fieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.IsoDate", fieldSchema.getProp("initial-type"));
    }

    @Test
    void isoTime() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.IsoTime");
        var inputValue = new Utf8("10:30:00+00:00");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        int expectedMillis = (int) OffsetTime.parse("10:30:00+00:00").getLong(ChronoField.MILLI_OF_DAY);
        assertEquals(expectedMillis, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-millis", fieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.IsoTime", fieldSchema.getProp("initial-type"));
    }

    @Test
    void isoTimestamp() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.IsoTimestamp");
        var inputValue = new Utf8("2024-01-15T10:30:00Z");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        var odt = OffsetDateTime.parse("2024-01-15T10:30:00Z");
        long expectedMicros = odt.toInstant().getEpochSecond() * 1_000_000L + odt.toInstant().getNano() / 1_000L;
        assertEquals(expectedMicros, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-micros", fieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.IsoTimestamp", fieldSchema.getProp("initial-type"));
    }

    @Test
    void microDuration() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.MicroDuration");
        long inputValue = 12345678L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals("12345678", result.value().get(FIELD_NAME));
        var microDurationFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals(Schema.Type.STRING, microDurationFieldSchema.getType());
        assertEquals("io.debezium.time.MicroDuration", microDurationFieldSchema.getProp("initial-type"));
    }

    @Test
    void interval() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.time.Interval");
        var inputValue = new Utf8("P1Y2M3DT4H5M6S");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals("P1Y2M3DT4H5M6S", result.value().get(FIELD_NAME));
        var intervalFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals(Schema.Type.STRING, intervalFieldSchema.getType());
        assertEquals("io.debezium.time.Interval", intervalFieldSchema.getProp("initial-type"));
    }

    @Test
    void debeziumDate() {
        var inputSchema = connectSchema(Schema.Type.INT, "io.debezium.time.Date");
        int inputValue = 19737;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(19737, result.value().get(FIELD_NAME));
        var dateFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("date", dateFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.Date", dateFieldSchema.getProp("initial-type"));
    }

    @Test
    void debeziumTime() {
        var inputSchema = connectSchema(Schema.Type.INT, "io.debezium.time.Time");
        int inputValue = 37800000;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(37800000, result.value().get(FIELD_NAME));
        var timeFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-millis", timeFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.Time", timeFieldSchema.getProp("initial-type"));
    }

    @Test
    void microTime() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.MicroTime");
        long inputValue = 37800000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(37800000000L, result.value().get(FIELD_NAME));
        var microTimeFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-micros", microTimeFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.MicroTime", microTimeFieldSchema.getProp("initial-type"));
    }

    @Test
    void nanoTime() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.NanoTime");
        long inputNanos = 37800000000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputNanos);

        long expectedMicros = inputNanos / 1_000L;
        assertEquals(expectedMicros, result.value().get(FIELD_NAME));
        var nanoTimeFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-micros", nanoTimeFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.NanoTime", nanoTimeFieldSchema.getProp("initial-type"));
    }

    @Test
    void timestamp() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.Timestamp");
        long inputValue = 1705312200000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000L, result.value().get(FIELD_NAME));
        var timestampFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-millis", timestampFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.Timestamp", timestampFieldSchema.getProp("initial-type"));
    }

    @Test
    void microTimestamp() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.MicroTimestamp");
        long inputValue = 1705312200000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000000L, result.value().get(FIELD_NAME));
        var microTsFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-micros", microTsFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.MicroTimestamp", microTsFieldSchema.getProp("initial-type"));
    }

    @Test
    void nanoTimestamp() {
        var inputSchema = connectSchema(Schema.Type.LONG, "io.debezium.time.NanoTimestamp");
        long inputNanos = 1705312200000000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputNanos);

        long expectedMicros = inputNanos / 1_000L;
        assertEquals(expectedMicros, result.value().get(FIELD_NAME));
        var nanoTsFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-micros", nanoTsFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.NanoTimestamp", nanoTsFieldSchema.getProp("initial-type"));
    }

    @Test
    void json() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.data.Json");
        var inputValue = new Utf8("{\"a\":1}");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        assertEquals("io.debezium.data.Json", result.valueSchema().getField(FIELD_NAME).schema().getProp("initial-type"));
    }

    @Test
    void xml() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.data.Xml");
        var inputValue = new Utf8("<root/>");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        assertEquals("io.debezium.data.Xml", result.valueSchema().getField(FIELD_NAME).schema().getProp("initial-type"));
    }

    @Test
    void uuid() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.data.Uuid");
        var inputValue = new Utf8("550e8400-e29b-41d4-a716-446655440000");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        var uuidFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("uuid", uuidFieldSchema.getLogicalType().getName());
        assertEquals("io.debezium.data.Uuid", uuidFieldSchema.getProp("initial-type"));
    }

    @Test
    void enumType() {
        var inputSchema = connectSchema(Schema.Type.STRING, "io.debezium.data.Enum");
        var inputValue = new Utf8("ACTIVE");

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        assertEquals("io.debezium.data.Enum", result.valueSchema().getField(FIELD_NAME).schema().getProp("initial-type"));
    }

    @Test
    void variableScaleDecimal() {
        var vsdSchema = Schema.createRecord("VariableScaleDecimal", null, "io.debezium.data", false, List.of(
                new Schema.Field("value", Schema.create(Schema.Type.BYTES)),
                new Schema.Field("scale", Schema.create(Schema.Type.INT))));
        vsdSchema.addProp("connect.name", "io.debezium.data.VariableScaleDecimal");

        var vsdValue = new GenericData.Record(vsdSchema);
        vsdValue.put("value", ByteBuffer.wrap(new BigInteger("123").toByteArray()));
        vsdValue.put("scale", 2);

        var result = deserializeInsert(FIELD_NAME, vsdSchema, vsdValue);

        assertEquals(1.23, (double) result.value().get(FIELD_NAME), 0.0001);
        var vsdFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals(Schema.Type.DOUBLE, vsdFieldSchema.getType());
        assertEquals("io.debezium.data.VariableScaleDecimal", vsdFieldSchema.getProp("initial-type"));
    }

    @Test
    void kafkaConnectDecimal() {
        var inputSchema = Schema.create(Schema.Type.BYTES);
        LogicalTypes.decimal(10, 2).addToSchema(inputSchema);
        inputSchema.addProp("connect.name", "org.apache.kafka.connect.data.Decimal");

        var inputValue = ByteBuffer.wrap(new BigInteger("12345").toByteArray());

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        var decimal = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
        assertEquals(10, decimal.getPrecision());
        assertEquals(2, decimal.getScale());
        assertEquals("org.apache.kafka.connect.data.Decimal", fieldSchema.getProp("initial-type"));
    }

    @Test
    void kafkaConnectDate() {
        var inputSchema = connectSchema(Schema.Type.INT, "org.apache.kafka.connect.data.Date");
        int inputValue = 19737;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(19737, result.value().get(FIELD_NAME));
        var kcDateFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("date", kcDateFieldSchema.getLogicalType().getName());
        assertEquals("org.apache.kafka.connect.data.Date", kcDateFieldSchema.getProp("initial-type"));
    }

    @Test
    void kafkaConnectTime() {
        var inputSchema = connectSchema(Schema.Type.INT, "org.apache.kafka.connect.data.Time");
        int inputValue = 37800000;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(37800000, result.value().get(FIELD_NAME));
        var kcTimeFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-millis", kcTimeFieldSchema.getLogicalType().getName());
        assertEquals("org.apache.kafka.connect.data.Time", kcTimeFieldSchema.getProp("initial-type"));
    }

    @Test
    void kafkaConnectTimestamp() {
        var inputSchema = connectSchema(Schema.Type.LONG, "org.apache.kafka.connect.data.Timestamp");
        long inputValue = 1705312200000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000L, result.value().get(FIELD_NAME));
        var kcTsFieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-millis", kcTsFieldSchema.getLogicalType().getName());
        assertEquals("org.apache.kafka.connect.data.Timestamp", kcTsFieldSchema.getProp("initial-type"));
    }

    @Test
    void avroTimeMillis() {
        var inputSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        int inputValue = 37800000;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(37800000, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-millis", fieldSchema.getLogicalType().getName());
        assertEquals(false, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MILLIS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void avroTimeMicros() {
        var inputSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        long inputValue = 37800000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(37800000000L, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("time-micros", fieldSchema.getLogicalType().getName());
        assertEquals(false, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MICROS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void avroTimestampMillis() {
        var inputSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        long inputValue = 1705312200000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000L, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-millis", fieldSchema.getLogicalType().getName());
        assertEquals(true, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MILLIS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void avroTimestampMicros() {
        var inputSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        long inputValue = 1705312200000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000000L, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertEquals("timestamp-micros", fieldSchema.getLogicalType().getName());
        assertEquals(true, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MICROS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void avroLocalTimestampMillis() {
        var inputSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        long inputValue = 1705312200000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000L, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertNotNull(fieldSchema.getLogicalType());
        assertEquals(false, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MILLIS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void avroLocalTimestampMicros() {
        var inputSchema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        long inputValue = 1705312200000000L;

        var result = deserializeInsert(FIELD_NAME, inputSchema, inputValue);

        assertEquals(1705312200000000L, result.value().get(FIELD_NAME));

        var fieldSchema = result.valueSchema().getField(FIELD_NAME).schema();
        assertNotNull(fieldSchema.getLogicalType());
        assertEquals(false, fieldSchema.getObjectProp("isAdjustedToUTC"));
        assertEquals("MICROS", fieldSchema.getProp("unit"));
        assertNull(fieldSchema.getProp("initial-type"));
    }

    @Test
    void nullableFieldWithValue() {
        var innerType = connectSchema(Schema.Type.STRING, "io.debezium.data.Json");
        var nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), innerType);

        var inputValue = new Utf8("{\"key\":\"val\"}");

        var result = deserializeInsert(FIELD_NAME, nullableSchema, inputValue);

        assertSame(inputValue, result.value().get(FIELD_NAME));
        assertEquals(Schema.Type.UNION, result.valueSchema().getField(FIELD_NAME).schema().getType());
    }

    @Test
    void nullableFieldNull() {
        var innerType = connectSchema(Schema.Type.STRING, "io.debezium.data.Json");
        var nullableSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), innerType);

        var result = deserializeInsert(FIELD_NAME, nullableSchema, null);

        assertNull(result.value().get(FIELD_NAME));
    }

    @Test
    void arrayField() {
        var arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
        var inputValue = List.of(new Utf8("a"), new Utf8("b"), new Utf8("c"));

        var result = deserializeInsert(FIELD_NAME, arraySchema, inputValue);

        var outputList = (List<?>) result.value().get(FIELD_NAME);
        assertEquals(3, outputList.size());
        assertEquals(new Utf8("a"), outputList.get(0));
        assertEquals(new Utf8("b"), outputList.get(1));
        assertEquals(new Utf8("c"), outputList.get(2));
    }

    @Test
    void mapField() {
        var mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
        var inputValue = Map.of(new Utf8("x"), 10, new Utf8("y"), 20);

        var result = deserializeInsert(FIELD_NAME, mapSchema, inputValue);

        var outputMap = (Map<?, ?>) result.value().get(FIELD_NAME);
        assertEquals(10, outputMap.get(new Utf8("x")));
        assertEquals(20, outputMap.get(new Utf8("y")));
    }

    @Test
    void nestedRecord() {
        var nestedSchema = Schema.createRecord("Nested", null, "io.debezium.connector.postgresql", false, List.of(
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("age", Schema.create(Schema.Type.INT))));

        var nestedValue = new GenericData.Record(nestedSchema);
        nestedValue.put("name", new Utf8("Alice"));
        nestedValue.put("age", 30);

        var result = deserializeInsert(FIELD_NAME, nestedSchema, nestedValue);

        var outputNested = (GenericRecord) result.value().get(FIELD_NAME);
        assertEquals(new Utf8("Alice"), outputNested.get("name"));
        assertEquals(30, outputNested.get("age"));
    }

    @Test
    void nestedRecordWithLogicalType() {
        var dateFieldSchema = connectSchema(Schema.Type.INT, "io.debezium.time.Date");
        var nestedSchema = Schema.createRecord("Inner", null, "io.debezium.connector.postgresql", false, List.of(
                new Schema.Field("label", Schema.create(Schema.Type.STRING)),
                new Schema.Field("created", dateFieldSchema)));

        var nestedValue = new GenericData.Record(nestedSchema);
        nestedValue.put("label", new Utf8("test"));
        nestedValue.put("created", 19737);

        var result = deserializeInsert(FIELD_NAME, nestedSchema, nestedValue);

        var outputNested = (GenericRecord) result.value().get(FIELD_NAME);
        assertEquals(new Utf8("test"), outputNested.get("label"));
        assertEquals(19737, outputNested.get("created"));
        var createdSchema = outputNested.getSchema().getField("created").schema();
        assertEquals("date", createdSchema.getLogicalType().getName());
        assertEquals("io.debezium.time.Date", createdSchema.getProp("initial-type"));
    }

    @Test
    void multipleFieldsWithDifferentLogicalTypes() {
        var dateSchema = connectSchema(Schema.Type.INT, "io.debezium.time.Date");
        var jsonSchema = connectSchema(Schema.Type.STRING, "io.debezium.data.Json");

        var schema = innerSchema(List.of(
                new Schema.Field("day", dateSchema),
                new Schema.Field("payload", jsonSchema),
                new Schema.Field("count", Schema.create(Schema.Type.LONG))));

        var inner = new GenericData.Record(schema);
        inner.put("day", 19737);
        inner.put("payload", new Utf8("{\"x\":1}"));
        inner.put("count", 99L);

        var result = deserialize(envelope("c", inner));

        assertEquals(19737, result.value().get("day"));
        assertSame(inner.get("payload"), result.value().get("payload"));
        assertEquals(99L, result.value().get("count"));

        var daySchema = result.valueSchema().getField("day").schema();
        assertEquals("date", daySchema.getLogicalType().getName());
        assertEquals("io.debezium.time.Date", daySchema.getProp("initial-type"));
        assertEquals("io.debezium.data.Json", result.valueSchema().getField("payload").schema().getProp("initial-type"));
        assertEquals(Schema.Type.LONG, result.valueSchema().getField("count").schema().getType());
        assertNull(result.valueSchema().getField("count").schema().getProp("initial-type"));
    }
}
