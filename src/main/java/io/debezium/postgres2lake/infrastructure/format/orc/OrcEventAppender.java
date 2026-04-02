package io.debezium.postgres2lake.infrastructure.format.orc;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.convertToBytes;

public class OrcEventAppender implements EventAppender<OrcTableWriter> {
    @Override
    public void appendEvent(EventRecord event, OrcTableWriter writer) throws IOException {
        var batch = writer.batch();
        var row = batch.size;
        batch.size += 1;

        saveRecord(event.value(), writer.writer().getSchema(), row, batch);

        if (batch.size == batch.getMaxSize()) {
            writer.writer().addRowBatch(batch);
            batch.reset();
        }
    }

    @Override
    public void commitPendingEvents(OrcTableWriter writer) throws Exception {
        if (writer.batch().size != 0) {
            writer.writer().addRowBatch(writer.batch());
            writer.batch().reset();
        }

        writer.writer().close();
    }

    private void saveRecord(GenericRecord record, TypeDescription schema, int rowIdx, VectorizedRowBatch vector) {
        var avroFields = record.getSchema().getFields();
        var orcFields = schema.getChildren();

        for (var fieldIdx = 0; fieldIdx < avroFields.size(); fieldIdx++) {
            var avroField = avroFields.get(fieldIdx);
            var avroValue = record.get(fieldIdx);

            var columnVector = vector.cols[fieldIdx];
            var orcField = orcFields.get(fieldIdx);

            saveValue(avroField.schema(), avroValue, orcField, rowIdx, columnVector);
        }
    }

    private void saveValue(Schema schema, Object avroFieldValue, TypeDescription orcField, int rowIdx, ColumnVector columnVector) {
        if (avroFieldValue == null) {
            columnVector.isNull[rowIdx] = true;
            columnVector.noNulls = false;
            return;
        }

        var logicalType = schema.getLogicalType();
        if (logicalType != null) {
            saveLogicalValue(logicalType, schema, avroFieldValue, rowIdx, columnVector);
            return;
        }

        switch (schema.getType()) {
            case STRING, ENUM, BYTES, FIXED -> saveBinary(avroFieldValue, columnVector, rowIdx);
            case FLOAT, DOUBLE -> saveDouble(avroFieldValue, columnVector, rowIdx);
            case BOOLEAN, INT, LONG -> saveLong(avroFieldValue, columnVector, rowIdx);
            case UNION -> {
                // use first not null paimonSchema
                if (schema.getType() == Schema.Type.UNION) {
                    for (Schema s : schema.getTypes()) {
                        if (s.getType() != Schema.Type.NULL) saveValue(s, avroFieldValue, orcField, rowIdx, columnVector);
                    }
                }
            }
            case RECORD -> saveRecord(avroFieldValue, columnVector, orcField, rowIdx);
            case MAP -> saveMap(schema, avroFieldValue, columnVector, orcField, rowIdx);
            case ARRAY -> saveArray(schema, avroFieldValue, columnVector, orcField, rowIdx);
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        }
    }

    private void saveLogicalValue(LogicalType logicalType, Schema schema, Object avroFieldValue, int rowIdx, ColumnVector columnVector) {
        switch (logicalType) {
            case LogicalTypes.Decimal ignored -> saveDecimal(schema, avroFieldValue, columnVector, rowIdx);
            case LogicalTypes.Uuid ignored ->  saveBinary(avroFieldValue, columnVector, rowIdx);
            case LogicalTypes.TimeMicros ignored -> saveLong(avroFieldValue, columnVector, rowIdx);
            case LogicalTypes.TimeMillis ignored -> saveLong(avroFieldValue, columnVector, rowIdx);
            case LogicalTypes.TimestampMicros ignored -> writeTimestampColumn(avroFieldValue, columnVector, rowIdx, true);
            case LogicalTypes.TimestampMillis ignored -> writeTimestampColumn(avroFieldValue, columnVector, rowIdx, false);
            case LogicalTypes.LocalTimestampMicros ignored -> writeTimestampColumn(avroFieldValue, columnVector, rowIdx, true);
            case LogicalTypes.LocalTimestampMillis ignored -> writeTimestampColumn(avroFieldValue, columnVector, rowIdx, false);
            case LogicalTypes.Date ignored -> saveDate(avroFieldValue, columnVector, rowIdx);
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
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

    private void saveDecimal(Schema avroSchema, Object avroValue, ColumnVector vector, int rowIdx) {
        var bytes = convertToBytes(avroValue);
        var decimalLogicalType = (LogicalTypes.Decimal) avroSchema.getLogicalType();
        var scale = decimalLogicalType.getScale();

        var typedVector = (DecimalColumnVector) vector;
        typedVector.vector[rowIdx] = new HiveDecimalWritable(bytes, scale);
    }

    private void writeTimestampColumn(Object avroValue, ColumnVector vector, int rowIdx, boolean isMicros) {
        long raw = (long) avroValue;
        long millis = isMicros ? Math.floorDiv(raw, 1000) : raw;
        int nanos = isMicros ? (Math.floorMod(raw, 1000) * 1000) : 0;
        var typedVector = (TimestampColumnVector) vector;
        typedVector.time[rowIdx] = millis;
        typedVector.nanos[rowIdx] = nanos;
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
}
