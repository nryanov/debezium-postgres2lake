package io.debezium.postgres2lake.infrastructure.format.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;

// Expected to be created once per partition inside batch
public class PartitionedAppendOnlyWriter extends PartitionedWriter<Record> {
    private final PartitionKey partitionKey;
    private final InternalRecordWrapper valueWrapper;

    public PartitionedAppendOnlyWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);

        this.partitionKey = new PartitionKey(spec, schema);
        this.valueWrapper = new InternalRecordWrapper(schema.asStruct());
    }

    @Override
    protected PartitionKey partition(Record row) {
        partitionKey.partition(valueWrapper.wrap(row));
        return partitionKey;
    }
}
