package io.debezium.postgres2lake.iceberg.infrastructure.writer;

import io.debezium.postgres2lake.iceberg.infrastructure.EnrichedRecordWrapper;
import io.debezium.postgres2lake.iceberg.infrastructure.RecordProjection;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;

import java.io.IOException;

public abstract class AbstractEqualityDeleteWriter extends BaseTaskWriter<Record> {
    private final Schema valueSchema;
    private final Schema keySchema;
    private final InternalRecordWrapper valueWrapper;
    private final InternalRecordWrapper keyWrapper;
    private final RecordProjection keyProjection;

    public AbstractEqualityDeleteWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);

        this.valueSchema = schema;
        this.keySchema = TypeUtil.select(schema, schema.identifierFieldIds());

        this.valueWrapper = new InternalRecordWrapper(valueSchema.asStruct());
        this.keyWrapper = new InternalRecordWrapper(keySchema.asStruct());

        this.keyProjection = RecordProjection.create(valueSchema, keySchema);
    }

    abstract RowEqualityDeleteWriter route(Record row);

    protected final InternalRecordWrapper valueWrapper() {
        return valueWrapper;
    }

    @Override
    public void write(Record row) throws IOException {
        var enrichedRecord = (EnrichedRecordWrapper) row;
        var partitionWriter = route(row);

        switch (enrichedRecord.operation()) {
            case INSERT -> partitionWriter.write(row);
            case UPDATE -> {
                partitionWriter.deleteKey(keyProjection.wrap(row));
                partitionWriter.write(row);
            }
            case DELETE -> partitionWriter.deleteKey(keyProjection.wrap(row));
        }
    }

    public class RowEqualityDeleteWriter extends BaseEqualityDeltaWriter {
        public RowEqualityDeleteWriter(PartitionKey partition) {
            super(partition, valueSchema, keySchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return valueWrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(Record key) {
            return keyWrapper.wrap(key);
        }
    }
}
