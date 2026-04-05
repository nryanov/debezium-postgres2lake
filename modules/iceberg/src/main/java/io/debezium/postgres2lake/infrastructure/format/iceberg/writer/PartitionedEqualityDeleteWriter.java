package io.debezium.postgres2lake.infrastructure.format.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.Tasks;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PartitionedEqualityDeleteWriter extends AbstractEqualityDeleteWriter {
    private final PartitionKey partitionKey;
    private final Map<PartitionKey, RowEqualityDeleteWriter> writerPerPartition;

    public PartitionedEqualityDeleteWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema);

        this.partitionKey = new PartitionKey(spec, schema);
        this.writerPerPartition = new HashMap<>();
    }

    @Override
    RowEqualityDeleteWriter route(Record row) {
        partitionKey.partition(valueWrapper().wrap(row));

        var partitionWriter = writerPerPartition.get(partitionKey);
        if (partitionWriter == null) {
            var copiedKey = partitionKey.copy();
            partitionWriter = new RowEqualityDeleteWriter(copiedKey);
            writerPerPartition.put(copiedKey, partitionWriter);
        }

        return partitionWriter;
    }

    @Override
    public void close() throws IOException {
        Tasks.foreach(writerPerPartition.values())
                .throwFailureWhenFinished()
                .noRetry()
                .run(RowEqualityDeleteWriter::close, IOException.class);

        writerPerPartition.clear();
    }
}
