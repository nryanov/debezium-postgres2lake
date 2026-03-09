package io.debezium.postgres2lake.infrastructure.format.orc;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class AvroToOrcMapper {
    private static final DateTimeFormatter ISO_OFFSET_DATE_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE;
    private static final DateTimeFormatter ISO_OFFSET_TIME_FORMAT = DateTimeFormatter.ISO_OFFSET_TIME;
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public TypeDescription avroToOrcSchema(Schema schema) {
        var connectLogicalType = schema.getProp("connect.name");
        if (connectLogicalType != null) {
            return avroLogicalToOrcSchema(connectLogicalType, schema);
        }

        return switch (schema.getType()) {
            case INT -> TypeDescription.createInt();
            case STRING, ENUM -> TypeDescription.createString();
            case BOOLEAN -> TypeDescription.createBoolean();
            case LONG -> TypeDescription.createLong();
            case FLOAT -> TypeDescription.createFloat();
            case DOUBLE -> TypeDescription.createDouble();
            case FIXED, BYTES -> TypeDescription.createBinary();
            case UNION -> {
                // use first not null schema
                if (schema.getType() == Schema.Type.UNION) {
                    for (Schema s : schema.getTypes()) {
                        if (s.getType() != Schema.Type.NULL) yield avroToOrcSchema(s);
                    }
                }

                throw new IllegalArgumentException("Unsupported type");
            }
            case MAP -> TypeDescription.createMap(TypeDescription.createString(), avroToOrcSchema(schema.getValueType()));
            case ARRAY -> TypeDescription.createList(avroToOrcSchema(schema.getElementType()));
            case RECORD -> {
                var struct = TypeDescription.createStruct();
                for (var field : schema.getFields()) {
                    struct.addField(field.name(), avroToOrcSchema(field.schema()));
                }
                yield struct;
            }
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    private TypeDescription avroLogicalToOrcSchema(String connectLogicalType, Schema schema) {
        return switch (connectLogicalType) {
            // debezium logical type mappings
            case "io.debezium.data.Bits" -> TypeDescription.createBinary();
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> TypeDescription.createTimestamp(); // initial type: string (utc)
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime", "io.debezium.time.Interval" -> TypeDescription.createLong(); // initial type: string (utc)
            case "io.debezium.time.MicroDuration" -> TypeDescription.createLong(); // initial type: long
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Uuid", "io.debezium.data.Enum" -> TypeDescription.createString(); // initial type: string
            case "io.debezium.time.Date" -> TypeDescription.createDate(); // initial type: int
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> TypeDescription.createLong(); // initial type: int/long
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp" -> TypeDescription.createTimestampInstant(); // initial type: long
            case "io.debezium.time.IsoDate" -> TypeDescription.createDate(); // initial type: string (utc)
            case "io.debezium.data.VariableScaleDecimal" -> TypeDescription.createDecimal(); // unknown precision
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> {
                var decimal = (LogicalTypes.Decimal) schema.getLogicalType();
                yield TypeDescription
                        .createDecimal()
                        .withScale(decimal.getScale())
                        .withPrecision(decimal.getPrecision());
            }
            case "org.apache.kafka.connect.data.Date" -> TypeDescription.createDate(); // initial type: int
            case "org.apache.kafka.connect.data.Time" -> TypeDescription.createLong(); // initial type: long
            case "org.apache.kafka.connect.data.Timestamp" -> TypeDescription.createTimestampInstant(); // initial type: long
            // avro logical type mappings
            case "decimal" -> {
                var decimal = (LogicalTypes.Decimal) schema.getLogicalType();
                yield TypeDescription
                        .createDecimal()
                        .withScale(decimal.getScale())
                        .withPrecision(decimal.getPrecision());
            }
            case "uuid" -> TypeDescription.createString();
            case "date" -> TypeDescription.createDate();
            case "time-millis" -> TypeDescription.createInt();
            case "time-micros" -> TypeDescription.createLong();
            case "timestamp-millis", "timestamp-micros" -> TypeDescription.createTimestamp();
            case "local-timestamp-millis", "local-timestamp-micros" -> TypeDescription.createTimestampInstant();
            case "duration" -> TypeDescription.createBinary();
            default -> throw new IllegalArgumentException("Unknown avro logical type: " + connectLogicalType);
        };
    }

    public void saveValue(Schema schema, Object avroFieldValue, TypeDescription orcField, int rowIdx, ColumnVector columnVector) {
        if (avroFieldValue == null) {
            columnVector.isNull[rowIdx] = true;
            columnVector.noNulls = false;
            return;
        }

        var valueType = schema.getType().getName().toLowerCase();
        var connectLogicalType = schema.getProp("connect.name");
        if (connectLogicalType != null) {
            valueType = connectLogicalType;
        }

        // todo: correctly save milli/micro/nano time
        // vector[idx].time = micros (?)
        // vector[idx].nanos = nanos

        // todo: fix timestamp conversions
        switch (valueType) {
            // debezium logical type mappings
            case "io.debezium.data.Bits" -> saveBinary(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> saveIsoTimestamp(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime" -> saveIsoTime(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.MicroDuration" -> saveLong(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Uuid", "io.debezium.data.Enum" -> saveBinary(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.Date" -> saveDate(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> saveLong(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp" -> saveLocalTimestamp(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.time.IsoDate" -> saveIsoDate(avroFieldValue, columnVector, rowIdx);
            case "io.debezium.data.VariableScaleDecimal" -> saveVariableScaleDecimal(avroFieldValue, columnVector, rowIdx);
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> saveDecimal(schema, avroFieldValue, columnVector, rowIdx);
            case "org.apache.kafka.connect.data.Date" -> saveDate(avroFieldValue, columnVector, rowIdx);
            case "org.apache.kafka.connect.data.Time" -> saveLong(avroFieldValue, columnVector, rowIdx);
            case "org.apache.kafka.connect.data.Timestamp" -> saveLocalTimestamp(avroFieldValue, columnVector, rowIdx);
            // avro logical type mappings
            case "decimal" -> saveDecimal(schema, avroFieldValue, columnVector, rowIdx);
            case "uuid" -> saveBinary(avroFieldValue, columnVector, rowIdx);
            case "date" -> saveDate(avroFieldValue, columnVector, rowIdx);
            case "time-millis", "time-micros" -> saveLong(avroFieldValue, columnVector, rowIdx);
            case "timestamp-millis", "timestamp-micros" -> saveTimestamp(avroFieldValue, columnVector, rowIdx);
            case "local-timestamp-millis", "local-timestamp-micros" -> saveLocalTimestamp(avroFieldValue, columnVector, rowIdx);
            // standard avro types
            case "boolean", "int", "long" -> saveLong(avroFieldValue, columnVector, rowIdx);
            case "string", "enum", "bytes", "fixed" -> saveBinary(avroFieldValue, columnVector, rowIdx);
            case "float", "double" -> saveDouble(avroFieldValue, columnVector, rowIdx);
            case "union" -> {
                // use first not null schema
                if (schema.getType() == Schema.Type.UNION) {
                    for (Schema s : schema.getTypes()) {
                        if (s.getType() != Schema.Type.NULL) saveValue(s, avroFieldValue, orcField, rowIdx, columnVector);
                    }
                }
            }
            case "map" -> saveMap(schema, avroFieldValue, columnVector, orcField, rowIdx);
            case "record" -> saveRecord(avroFieldValue, columnVector, orcField, rowIdx);
            case "array" -> saveArray(schema, avroFieldValue, columnVector, orcField, rowIdx);
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        }
    }

    private void saveBinary(Object avroValue, ColumnVector vector, int rowIdx) {
        var typedVector = (BytesColumnVector) vector;
        var bytes = convertToBytes(avroValue);
        typedVector.setRef(rowIdx, bytes, 0, bytes.length);
    }

    private void saveDate(Object avroValue, ColumnVector vector, int rowIdx) {
        var epochDay = (int) avroValue;
        var typedVector = (DateColumnVector) vector;
        typedVector.vector[rowIdx] = epochDay;
    }

    private void saveIsoDate(Object avroValue, ColumnVector vector, int rowIdx) {
        var rawDate = convertToString(avroValue);
        // ISO format in debezium in UTC format
        var date = LocalDate.parse(rawDate, ISO_OFFSET_DATE_FORMAT);
        var typedVector = (DateColumnVector) vector;
        typedVector.vector[rowIdx] = date.toEpochDay();
    }

    private void saveIsoTimestamp(Object avroValue, ColumnVector vector, int rowIdx) {
        var rawTimestamp = convertToString(avroValue);
        // ISO format in debezium in UTC format
        var timestamp = OffsetDateTime.parse(rawTimestamp, ISO_TIMESTAMP_FORMAT);
        var instant = timestamp.toInstant();
        var typedVector = (TimestampColumnVector) vector;
        typedVector.time[rowIdx] = instant.toEpochMilli();
        typedVector.nanos[rowIdx] = instant.getNano();
    }

    private void saveIsoTime(Object avroValue, ColumnVector vector, int rowIdx) {
        var rawTime = convertToString(avroValue);
        // ISO format in debezium in UTC format
        var time = OffsetTime.parse(rawTime, ISO_OFFSET_TIME_FORMAT);
        var typedVector = (LongColumnVector) vector;
        typedVector.vector[rowIdx] = time.toLocalTime().toNanoOfDay();
    }

    private void saveDecimal(Schema avroSchema, Object avroValue, ColumnVector vector, int rowIdx) {
        var bytes = convertToBytes(avroValue);
        var decimalLogicalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
        var scale = decimalLogicalType.getScale();

        var typedVector = (DecimalColumnVector) vector;
        typedVector.vector[rowIdx] = new HiveDecimalWritable(bytes, scale);
    }

    private void saveVariableScaleDecimal(Object avroValue, ColumnVector vector, int rowIdx) {
        // Debezium VariableScaleDecimal is a struct: { "scale": int, "value": bytes }
        var record = (GenericRecord) avroValue;
        var scale = (Integer) record.get("scale");
        var valueBytes = (ByteBuffer) record.get("value");

        if (valueBytes == null) {
            vector.isNull[rowIdx] = true;
            vector.noNulls = false;
            return;
        }

        var bytes = new byte[valueBytes.remaining()];
        valueBytes.get(bytes);

        var typedVector = (DecimalColumnVector) vector;
        typedVector.vector[rowIdx] = new HiveDecimalWritable(bytes, scale);
    }

    private void saveTimestamp(Object avroValue, ColumnVector vector, int rowIdx) {
        var millis = (long) avroValue;
        var typedVector = (TimestampColumnVector) vector;
        typedVector.time[rowIdx] = millis;
        typedVector.nanos[rowIdx] = 0;
    }

    private void saveLocalTimestamp(Object avroValue, ColumnVector vector, int rowIdx) {
        var millis = (long) avroValue;
        var typedVector = (TimestampColumnVector) vector;
        typedVector.time[rowIdx] = millis;
        typedVector.nanos[rowIdx] = 0;
    }

    private void saveArray(Schema avroSchema, Object avroValue, ColumnVector vector, TypeDescription orcField, int rowIdx) {
        var typedVector = (ListColumnVector) vector;
        var list = (List<?>) avroValue;
        // last offset + previous length of list
        long offset = (rowIdx == 0) ? 0 : typedVector.offsets[rowIdx - 1] + typedVector.lengths[rowIdx - 1];
        typedVector.offsets[rowIdx] = offset;
        typedVector.lengths[rowIdx] = list.size();

        // Fill child vector
        var childVector = typedVector.child;
        var childSchema = orcField.getChildren().getFirst();
        for (var i = 0; i < list.size(); i++) {
            saveValue(avroSchema.getElementType(), list.get(i), childSchema, (int)(offset + i), childVector);
        }
    }

    private void saveMap(Schema avroSchema, Object avroValue, ColumnVector vector, TypeDescription orcField, int rowIdx) {
        var typedVector = (MapColumnVector) vector;
        var map = (Map<?, ?>) avroValue;
        long offset = (rowIdx == 0) ? 0 : typedVector.offsets[rowIdx - 1] + typedVector.lengths[rowIdx - 1];
        typedVector.offsets[rowIdx] = offset;
        typedVector.lengths[rowIdx] = map.size();

        int idx = 0;
        for (var entry : map.entrySet()) {
            saveValue(SchemaBuilder.builder().stringType(), entry.getKey(), orcField.getChildren().getFirst(), (int)(offset + idx), typedVector.keys);
            saveValue(avroSchema.getValueType(), entry.getValue(), orcField.getChildren().getLast(), (int)(offset + idx), typedVector.values);
            idx++;
        }
    }

    private void saveRecord(Object avroValue, ColumnVector vector, TypeDescription orcField, int rowIdx) {
        var typedVector = (StructColumnVector) vector;
        var nestedRecord = (GenericRecord) avroValue;
        var orcNestedFields = orcField.getChildren();

        for (int i = 0; i < orcNestedFields.size(); i++) {
            var nestedFieldType = orcNestedFields.get(i);
            var nestedFieldVector = typedVector.fields[i];
            var nestedAvroFieldValue = nestedRecord.get(nestedFieldType.getFullFieldName());
            var nestedAvroFieldSchema = nestedRecord.getSchema().getField(nestedFieldType.getFullFieldName()).schema();

            saveValue(nestedAvroFieldSchema, nestedAvroFieldValue, nestedFieldType, rowIdx, nestedFieldVector);
        }
    }

    private void saveDouble(Object avroValue, ColumnVector vector, int rowIdx) {
        var typedVector = (DoubleColumnVector) vector;
        typedVector.vector[rowIdx] = ((Number) avroValue).doubleValue();
    }

    private void saveLong(Object avroValue, ColumnVector vector, int rowIdx) {
        var typedVector = (LongColumnVector) vector;
        switch (avroValue) {
            case Boolean bool -> typedVector.vector[rowIdx] = bool ? 1 : 0;
            case Number number -> typedVector.vector[rowIdx] = number.longValue();
            default -> throw new IllegalArgumentException("Unexpected value for long: " + avroValue);
        }
    }

    private byte[] convertToBytes(Object value) {
        return switch (value) {
            case ByteBuffer buffer -> {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                yield bytes;
            }
            case byte[] bytes -> bytes;
            case GenericData.Fixed fixed -> fixed.bytes();
            default -> String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        };
    }

    private String convertToString(Object value) {
        return switch (value) {
            case Utf8 utf8 -> utf8.toString();
            default -> value.toString();
        };
    }
}
