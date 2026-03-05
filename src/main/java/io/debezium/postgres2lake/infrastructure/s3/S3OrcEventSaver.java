package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.orc.OrcOpenedWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class S3OrcEventSaver extends AbstractEventSaver<OrcOpenedWriter> {

    private static final Logger logger = Logger.getLogger(S3OrcEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;

    public S3OrcEventSaver(OutputLocationGenerator outputLocationGenerator, OutputConfiguration.FileIO fileIO) {
        super();
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
    }

    private void saveRecord(GenericRecord record, TypeDescription schema, int rowIdx, VectorizedRowBatch vector) {
        var orcFields = schema.getChildren();
        for (var fieldIdx = 0; fieldIdx < orcFields.size(); fieldIdx++) {
            var orcField = orcFields.get(fieldIdx);
            var avroFieldValue = record.get(fieldIdx);
            var columnVector = vector.cols[fieldIdx];

            saveValue(avroFieldValue, orcField, rowIdx, columnVector);
        }
    }

    private void saveValue(Object avroFieldValue, TypeDescription orcField, int rowIdx, ColumnVector columnVector) {
            if (avroFieldValue == null) {
                columnVector.isNull[rowIdx] = true;
                columnVector.noNulls = false;
            } else {
                columnVector.isNull[rowIdx] = false;

                switch (orcField.getCategory()) {
                    case TIMESTAMP, TIMESTAMP_INSTANT -> {
                        var typedVector = (TimestampColumnVector) columnVector;
                        long nanos;
                        if (avroFieldValue instanceof Long) {
                            nanos = ((Long) avroFieldValue) * 1_000_000;
                        } else if (avroFieldValue instanceof Instant) {
                            nanos = ((Instant) avroFieldValue).toEpochMilli() * 1_000_000;
                        } else {
                            throw new IllegalArgumentException("Unsupported timestamp type: " + avroFieldValue.getClass());
                        }
                        typedVector.time[rowIdx] = nanos;
                    }
                    case FLOAT, DOUBLE -> {
                        var typedVector = (DoubleColumnVector) columnVector;
                        typedVector.vector[rowIdx] = ((Number) avroFieldValue).doubleValue();
                    }
                    case BYTE, SHORT, INT, LONG, DATE, BOOLEAN -> {
                        var typedVector = (LongColumnVector) columnVector;
                        if (orcField.getCategory() == TypeDescription.Category.BOOLEAN) {
                            typedVector.vector[rowIdx] = (Boolean) avroFieldValue ? 1 : 0;
                        } else if (orcField.getCategory() == TypeDescription.Category.DATE) {
                            // Avro Date: days since epoch (int), ORC Date: days since epoch (long)
                            typedVector.vector[rowIdx] = ((Number) avroFieldValue).longValue();
                        } else {
                            typedVector.vector[rowIdx] = ((Number) avroFieldValue).longValue();
                        }
                    }
                    case CHAR, STRING, VARCHAR, BINARY -> {
                        var typedVector = (BytesColumnVector) columnVector;
                        byte[] bytes;
                        if (avroFieldValue instanceof ByteBuffer buffer) {
                            bytes = new byte[buffer.remaining()];
                            buffer.duplicate().get(bytes);
                        } else if (avroFieldValue instanceof CharSequence) {
                            bytes = avroFieldValue.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        } else {
                            bytes = (byte[]) avroFieldValue;
                        }
                        typedVector.setRef(rowIdx, bytes, 0, bytes.length);
                    }
                    case DECIMAL -> {
                        // todo: add support
                    }
                    case LIST -> {
                        var typedVector = (ListColumnVector) columnVector;
                        var list = (List<?>) avroFieldValue;
                        // last offset + previous length of list
                        long offset = (rowIdx == 0) ? 0 : typedVector.offsets[rowIdx - 1] + typedVector.lengths[rowIdx - 1];
                        typedVector.offsets[rowIdx] = offset;
                        typedVector.lengths[rowIdx] = list.size();

                        // Fill child vector
                        var childVector = typedVector.child;
                        var childSchema = orcField.getChildren().getFirst();
                        for (var i = 0; i < list.size(); i++) {
                            saveValue(list.get(i), childSchema, (int)(offset + i), childVector);
                        }
                    }
                    case MAP -> {
                        var typedVector = (MapColumnVector) columnVector;
                        var map = (Map<?, ?>) avroFieldValue;
                        long offset = (rowIdx == 0) ? 0 : typedVector.offsets[rowIdx - 1] + typedVector.lengths[rowIdx - 1];
                        typedVector.offsets[rowIdx] = offset;
                        typedVector.lengths[rowIdx] = map.size();

                        int idx = 0;
                        for (var entry : map.entrySet()) {
                            saveValue(entry.getKey(), orcField.getChildren().getFirst(), (int)(offset + idx), typedVector.keys);
                            saveValue(entry.getValue(), orcField.getChildren().getLast(), (int)(offset + idx), typedVector.values);
                            idx++;
                        }
                    }
                    case STRUCT -> {
                        var typedVector = (StructColumnVector) columnVector;
                        var nestedRecord = (GenericRecord) avroFieldValue;
                        var orcNestedFields = orcField.getChildren();

                        for (int i = 0; i < orcNestedFields.size(); i++) {
                            var nestedFieldType = orcNestedFields.get(i);
                            var nestedFieldVector = typedVector.fields[i];
                            var nestedAvroFieldValue = nestedRecord.get(nestedFieldType.getFullFieldName());
                            saveValue(nestedAvroFieldValue, nestedFieldType, rowIdx, nestedFieldVector);
                        }
                    }
                    case null, default -> throw new IllegalArgumentException("Unsupported type");
                }
            }
    }

    // todo: use ORCSchemaUtils from iceberg?
    private TypeDescription avroToOrcSchema(Schema schema) {
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
                for (var field :  schema.getFields()) {
                    struct.addField(field.name(), avroToOrcSchema(field.schema()));
                }
                yield struct;
            }
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    @Override
    protected OrcOpenedWriter createWriter(EventRecord event) {
        var location = outputLocationGenerator.generateLocation("warehouse", event);
        var writer = createFileWriter(location, avroToOrcSchema(event.value().getSchema()));
        var batch = writer.getSchema().createRowBatch(); // todo: configure batch size

        return new OrcOpenedWriter(writer, batch);
    }

    private Writer createFileWriter(String location, TypeDescription schema) {
        try {
            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var options = OrcFile.writerOptions(config)
                    .setSchema(schema)
                    .stripeSize(64 * 1024 * 1024) // 64 Mb
                    .useUTCTimestamp(true)
                    .compress(CompressionKind.NONE);


            return OrcFile.createWriter(new Path(location), options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, OrcOpenedWriter writer) {
        try {
            var batch = writer.batch();
            var row = batch.size;
            batch.size += 1;

            saveRecord(event.value(), writer.writer().getSchema(), row, batch);

            if (batch.size == batch.getMaxSize()) {
                writer.writer().addRowBatch(batch);
                batch.reset();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commitPendingEvents(OrcOpenedWriter writer) {
        try {
            if (writer.batch().size != 0) {
                logger.infof("Add rows batch: %s", writer.batch().count());
                writer.writer().addRowBatch(writer.batch());
                writer.batch().reset();
            }

            writer.writer().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
