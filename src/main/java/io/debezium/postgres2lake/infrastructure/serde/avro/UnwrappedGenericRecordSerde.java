package io.debezium.postgres2lake.infrastructure.serde.avro;

import io.debezium.engine.ChangeEvent;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.*;

@ApplicationScoped
public class UnwrappedGenericRecordSerde {
    private static final String CONNECT_TYPE_NAME = "connect.name";
    private static final String INITIAL_TYPE_NAME = "initial-type";

    private static final String PARQUET_ADJUST_TO_UTC = "isAdjustedToUTC";
    private static final String PARQUET_UNIT = "unit";
    private static final String ICEBERG_ADJUST_TO_UTC = "adjust-to-utc";

    private static final String MILLIS_UNIT = "MILLIS";
    private static final String MICROS_UNIT = "MICROS";
    private static final String NANOS_UNIT = "NANOS";

    private static final String OPERATION_FIELD_NAME = "op";
    private static final String AFTER_FIELD_NAME = "after";
    private static final String BEFORE_FIELD_NAME = "before";
    private static final String SOURCE_FIELD_NAME = "source";
    private static final String SOURCE_LSN_FIELD_NAME = "lsn";
    private static final String SOURCE_TS_MS_FIELD_NAME = "ts_ms";

    private static final Set<String> SYSTEM_FIELDS = Set.of(
            EventRecord.UNWRAPPED_OPERATION_FIELD_NAME,
            EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME,
            EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME
    );

    private final GenericRecordSerde serde;

    public UnwrappedGenericRecordSerde(GenericRecordSerde serde) {
        this.serde = serde;
    }

    public EventRecord deserialize(ChangeEvent<Object, Object> event) {
        var keyPart = (byte[]) event.key();
        var valuePart = (byte[]) event.value();

        var key = serde.deserialize(keyPart);
        var value = serde.deserialize(valuePart);
        var operation = resolveOperation(value);
        var unwrappedValue = unwrap(operation, value);

        return new EventRecord(operation, key, unwrappedValue, event.destination());
    }

    private Operation resolveOperation(GenericRecord payload) {
        var op = ((Utf8) payload.get(OPERATION_FIELD_NAME)).toString();
        return switch (op) {
            case "c", "r" -> Operation.INSERT;
            case "u" -> Operation.UPDATE;
            case "d" -> Operation.DELETE;
            case null, default -> throw new IllegalArgumentException("unknown operation: " + op);
        };
    }

    private GenericRecord unwrap(Operation operation, GenericRecord payload) {
        var sourcePart = (GenericRecord) payload.get(SOURCE_FIELD_NAME);
        var lsn = (Long) sourcePart.get(SOURCE_LSN_FIELD_NAME);
        var timestampMillis = (Long) sourcePart.get(SOURCE_TS_MS_FIELD_NAME);

        var unwrappedValues = switch (operation) {
            case INSERT, UPDATE -> (GenericRecord) payload.get(AFTER_FIELD_NAME);
            case DELETE -> (GenericRecord) payload.get(BEFORE_FIELD_NAME);
        };

        var unwrappedSchema = getOrCreateUnwrappedSchema(payload, unwrappedValues);
        var unwrappedRecord = new GenericData.Record(unwrappedSchema);

        for (var field : unwrappedSchema.getFields()) {
            if (SYSTEM_FIELDS.contains(field.name())) {
                continue;
            }

            unwrappedRecord.put(field.name(), normalizeValue(field.schema(), unwrappedValues.get(field.name())));
        }

        unwrappedRecord.put(EventRecord.UNWRAPPED_OPERATION_FIELD_NAME, operation.name());
        unwrappedRecord.put(EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME, lsn);
        unwrappedRecord.put(EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME, timestampMillis);

        return unwrappedRecord;
    }

    private Schema getOrCreateUnwrappedSchema(GenericRecord payload, GenericRecord values) {
        var initialSchema = payload.getSchema();
        var valuesSchema = normalizeSchema(values.getSchema());

        var schemaBuilder = SchemaBuilder
                .builder()
                .record(initialSchema.getName())
                .namespace(initialSchema.getNamespace())
                .fields();

        schemaBuilder.requiredString(EventRecord.UNWRAPPED_OPERATION_FIELD_NAME);
        schemaBuilder.requiredLong(EventRecord.UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME);
        schemaBuilder.requiredLong(EventRecord.UNWRAPPED_EVENT_TIME_FIELD_NAME);

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

    private Object normalizeValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        return switch (schema.getType()) {
            case RECORD -> {
                var record = new GenericData.Record(schema);
                var currentValueRecord = (GenericRecord) value;

                for (var field : schema.getFields()) {
                    var currentValue = currentValueRecord.get(field.name());
                    record.put(field.name(), normalizeValue(field.schema(), currentValue));
                }

                yield record;
            }
            case UNION -> {
                var unwrappedUnion = unwrapUnion(schema);
                yield normalizeValue(unwrappedUnion, value);
            }
            case MAP -> {
                var rawMap = (Map<?, ?>) value;
                var targetMap = new HashMap<>();

                rawMap.forEach((key, rawValue) -> targetMap.put(key, normalizeValue(schema.getValueType(), rawValue)));

                yield targetMap;
            }
            case ARRAY -> {
                var rawArray = (List<?>) value;
                var mappedArray = new ArrayList<>();

                rawArray.forEach(it -> mappedArray.add(normalizeValue(schema.getElementType(), it)));

                yield mappedArray;
            }
            default -> normalizePrimitiveValue(schema, value);
        };
    }

    private Object normalizePrimitiveValue(Schema schema, Object value) {
        var initialType = schema.getProp(INITIAL_TYPE_NAME);
        if (initialType != null) {
            return normalizeConnectLogicalTypeValue(initialType, value);
        }

        // return as-is because paimonSchema is already has needed metadata
        return value;
    }

    private Object normalizeConnectLogicalTypeValue(String initialType, Object value) {
        return switch (initialType) {
            case "io.debezium.data.Bits" -> value;
            case "io.debezium.time.ZonedTimestamp" -> timestampToMicroseconds(parseIsoTimestamp(value));
            case "io.debezium.time.ZonedTime" -> millisSinceMidnight(parseIsoTime(value));
            case "io.debezium.time.IsoDate" -> dateToEpochDays(parseIsoDate(value));
            case "io.debezium.time.IsoTime" -> millisSinceMidnight(parseIsoTime(value));
            case "io.debezium.time.IsoTimestamp" -> timestampToMicroseconds(parseIsoTimestamp(value));
            case "io.debezium.time.MicroDuration" -> convertToString(value);
            case "io.debezium.time.Interval" -> convertToString(value);
            case "io.debezium.time.Date" -> value;
            case "io.debezium.time.Time" -> value;
            case "io.debezium.time.MicroTime" -> value;
            case "io.debezium.time.NanoTime" -> nanoToMicros(value);
            case "io.debezium.time.Timestamp" -> value;
            case "io.debezium.time.MicroTimestamp" -> value;
            case "io.debezium.time.NanoTimestamp" -> nanoToMicros(value);
            case "io.debezium.data.Json" -> value;
            case "io.debezium.data.Xml" -> value;
            case "io.debezium.data.Uuid" -> value;
            case "io.debezium.data.Enum" -> value;
            case "io.debezium.data.VariableScaleDecimal" -> variableDecimalValue(value);
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> value;
            case "org.apache.kafka.connect.data.Date" -> value;
            case "org.apache.kafka.connect.data.Time" -> value;
            case "org.apache.kafka.connect.data.Timestamp" -> value;
            default -> value; // unexpected type -> return as is
        };
    }

    private long nanoToMicros(Object value) {
        var nanos = (long) value;

        return nanos / 1_000L;
    }

    private long timestampToMicroseconds(OffsetDateTime odt) {
        var instant = odt.toInstant();
        var seconds = instant.getEpochSecond();
        var nanos = instant.getNano();

        return seconds * 1_000_000L + nanos / 1_000L;
    }

    private int dateToEpochDays(LocalDate ld) {
        return (int) ld.toEpochDay();
    }

    private int millisSinceMidnight(OffsetTime ot) {
        return (int) ot.getLong(ChronoField.MILLI_OF_DAY);
    }

    private double variableDecimalValue(Object value) {
        var struct = (GenericRecord) value;
        var valueBytes = (ByteBuffer) struct.get("value");
        var scale = (int) struct.get("scale");

        var decimal = new BigDecimal(new BigInteger(valueBytes.array()), scale);
        return decimal.doubleValue();
    }

    private Schema normalizeSchema(Schema schema) {
        return switch (schema.getType()) {
            case RECORD -> {
                var initialType = schema.getProp(CONNECT_TYPE_NAME);
                // special case for VariableScaleDecimal because it is a record
                if ("io.debezium.data.VariableScaleDecimal".equals(initialType)) {
                    yield  normalizePrimitive(schema);
                }

                var builder = SchemaBuilder
                        .record(schema.getName())
                        .namespace(schema.getNamespace())
                        .doc(schema.getDoc())
                        .fields();

                for (var field : schema.getFields()) {
                    var normalizedFieldSchema = normalizeSchema(field.schema());
                    var fieldBuilder = builder.name(field.name()).doc(field.doc()).type(normalizedFieldSchema);

                    if (field.hasDefaultValue() && field.defaultVal() != JsonProperties.NULL_VALUE) {
                        fieldBuilder.withDefault(field.defaultVal());
                    } else if (field.defaultVal() == JsonProperties.NULL_VALUE) {
                        fieldBuilder.withDefault(null);
                    } else {
                        fieldBuilder.noDefault();
                    }
                }

                yield builder.endRecord();
            }
            case ARRAY -> SchemaBuilder.array().items(normalizeSchema(schema.getElementType()));
            case MAP -> SchemaBuilder.map().values(normalizeSchema(schema.getValueType()));
            case UNION -> {
                var innerSchema = unwrapUnion(schema);
                yield SchemaBuilder
                        .unionOf()
                        .nullType()
                        .and()
                        .type(normalizeSchema(innerSchema))
                        .endUnion();

            }
            default -> normalizePrimitive(schema);
        };
    }

    private Schema normalizePrimitive(Schema schema) {
        var connectLogicalType = schema.getProp(CONNECT_TYPE_NAME);
        if (connectLogicalType != null) {
            return normalizeConnectLogicalTypes(connectLogicalType, schema);
        }

        var logicalType = schema.getLogicalType();
        if (logicalType != null) {
            return normalizeLogicalTypes(logicalType, schema);
        }

        return schema;
    }

    private Schema normalizeConnectLogicalTypes(String connectLogicalType, Schema schema) {
        return switch (connectLogicalType) {
            case "io.debezium.data.Bits" -> {
                var newSchema = SchemaBuilder.builder().bytesType();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.Bits");
                yield newSchema;
            }
            case "io.debezium.time.ZonedTimestamp" -> {
                var newSchema = normalizedTimestamp(true, MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.ZonedTimestamp");
                yield newSchema;
            }
            case "io.debezium.time.ZonedTime" -> {
                var newSchema = normalizedTime(MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.ZonedTime");
                yield newSchema;
            }
            case "io.debezium.time.IsoDate" -> {
                var newSchema = dateSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.IsoDate");
                yield newSchema;
            }
            case "io.debezium.time.IsoTime" -> {
                var newSchema = normalizedTime(MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.IsoTime");
                yield newSchema;
            }
            case "io.debezium.time.IsoTimestamp" -> {
                var newSchema = normalizedTimestamp(true, MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.IsoTimestamp");
                yield newSchema;
            }
            case "io.debezium.time.MicroDuration" -> {
                var newSchema = intervalSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.MicroDuration");
                yield newSchema;
            }
            case "io.debezium.time.Interval" -> {
                var newSchema = intervalSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.Interval");
                yield newSchema;
            }
            case "io.debezium.time.Date" -> {
                var newSchema = dateSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.Date");
                yield newSchema;
            }
            case "io.debezium.time.Time" -> {
                var newSchema = normalizedTime(MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.Time");
                yield newSchema;
            }
            case "io.debezium.time.MicroTime" -> {
                var newSchema = normalizedTime(MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.MicroTime");
                yield newSchema;
            }
            case "io.debezium.time.NanoTime" -> {
                var newSchema = normalizedTime(MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.NanoTime");
                yield newSchema;
            } // currently only MILLIS/MICROS types are supported
            case "io.debezium.time.Timestamp" -> {
                var newSchema = normalizedTimestamp(false, MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.Timestamp");
                yield newSchema;
            }
            case "io.debezium.time.MicroTimestamp" -> {
                var newSchema = normalizedTimestamp(false, MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.MicroTimestamp");
                yield newSchema;
            }
            case "io.debezium.time.NanoTimestamp" -> {
                var newSchema = normalizedTimestamp(false, MICROS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.time.NanoTimestamp");
                yield newSchema;
            }
            case "io.debezium.data.Json" -> {
                var newSchema = SchemaBuilder.builder().stringType();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.Json");
                yield newSchema;
            }
            case "io.debezium.data.Xml" -> {
                var newSchema = SchemaBuilder.builder().stringType();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.Xml");
                yield newSchema;
            }
            case "io.debezium.data.Uuid" -> {
                var newSchema = uuidSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.Uuid");
                yield newSchema;
            }
            case "io.debezium.data.Enum" -> {
                var newSchema = SchemaBuilder.builder().stringType();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.Enum");
                yield newSchema;
            }
            case "io.debezium.data.VariableScaleDecimal" -> {
                var newSchema = variableDcimalSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "io.debezium.data.VariableScaleDecimal");
                yield newSchema;
            }
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> {
                var newSchema = decimalSchema(schema);
                newSchema.addProp(INITIAL_TYPE_NAME, "org.apache.kafka.connect.data.Decimal");
                yield newSchema;
            }
            case "org.apache.kafka.connect.data.Date" -> {
                var newSchema = dateSchema();
                newSchema.addProp(INITIAL_TYPE_NAME, "org.apache.kafka.connect.data.Date");
                yield newSchema;
            }
            case "org.apache.kafka.connect.data.Time" -> {
                var newSchema = normalizedTime(MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "org.apache.kafka.connect.data.Time");
                yield newSchema;
            }
            case "org.apache.kafka.connect.data.Timestamp" -> {
                var newSchema = normalizedTimestamp(false, MILLIS_UNIT);
                newSchema.addProp(INITIAL_TYPE_NAME, "org.apache.kafka.connect.data.Timestamp");
                yield newSchema;
            }
            default -> schema; // unexpected type -> return as is
        };
    }

    private Schema normalizeLogicalTypes(LogicalType logicalType, Schema schema) {
        return switch (logicalType.getName()) {
            case "time-millis" -> normalizedTime(MILLIS_UNIT);
            case "time-micros" -> normalizedTime(MICROS_UNIT);
            case "timestamp-millis" -> normalizedTimestamp(true, MILLIS_UNIT);
            case "timestamp-micros" -> normalizedTimestamp(true, MICROS_UNIT);
            case "local-timestamp-millis" -> normalizedTimestamp(false, MILLIS_UNIT);
            case "local-timestamp-micros" -> normalizedTimestamp(false, MICROS_UNIT);
            default -> schema;
        };
    }

    private Schema normalizedTime(String unit) {
        return switch (unit) {
            case MILLIS_UNIT -> {
                var schema = SchemaBuilder
                        .builder()
                        .intBuilder()
                        // time is always without TZ
                        .prop(PARQUET_ADJUST_TO_UTC, false)
                        .prop(ICEBERG_ADJUST_TO_UTC, false)
                        .prop(PARQUET_UNIT, unit)
                        .endInt();
                yield LogicalTypes.timeMillis().addToSchema(schema);
            }
            default -> {
                var schema = SchemaBuilder
                        .builder()
                        .longBuilder()
                        // time is always without TZ
                        .prop(PARQUET_ADJUST_TO_UTC, false)
                        .prop(ICEBERG_ADJUST_TO_UTC, false)
                        .prop(PARQUET_UNIT, unit)
                        .endLong();
                yield LogicalTypes.timeMicros().addToSchema(schema);
            }
        };
    }

    private Schema normalizedTimestamp(boolean adjustToUtc, String unit) {
        var schema = SchemaBuilder
                .builder()
                .longBuilder()
                .prop(PARQUET_ADJUST_TO_UTC, adjustToUtc)
                .prop(ICEBERG_ADJUST_TO_UTC, adjustToUtc)
                .prop(PARQUET_UNIT, unit)
                .endLong();

        return switch (unit) {
            case MILLIS_UNIT -> LogicalTypes.timestampMillis().addToSchema(schema);
            // ignore nano unit logical type
            default -> LogicalTypes.timestampMicros().addToSchema(schema);
        };
    }

    private Schema decimalSchema(Schema decimal) {
        var decimalLogicalType = (LogicalTypes.Decimal) decimal.getLogicalType();
        return LogicalTypes
                .decimal(decimalLogicalType.getPrecision(), decimalLogicalType.getScale())
                .addToSchema(SchemaBuilder.builder().bytesType());
    }

    private Schema variableDcimalSchema() {
        return SchemaBuilder.builder().doubleType();
    }

    private Schema dateSchema() {
        return LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
    }

    private Schema uuidSchema() {
        return LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType());
    }

    // for now return just string
    private Schema intervalSchema() {
        return SchemaBuilder.builder().stringType();
    }

    private Schema unwrapUnion(Schema schema) {
        for (Schema s : schema.getTypes()) {
            if (s.getType() != Schema.Type.NULL) return s;
        }

        throw new IllegalArgumentException("Unexpected union type: " + schema);
    }
}
