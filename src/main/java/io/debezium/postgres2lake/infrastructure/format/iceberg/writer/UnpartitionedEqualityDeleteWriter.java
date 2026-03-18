package io.debezium.postgres2lake.infrastructure.format.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;

import java.io.IOException;

public class UnpartitionedEqualityDeleteWriter extends AbstractEqualityDeleteWriter {
    private final RowEqualityDeleteWriter writer;

    public UnpartitionedEqualityDeleteWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema);
        // create unpartitioned writer
        this.writer = new RowEqualityDeleteWriter(null);
    }

    @Override
    RowEqualityDeleteWriter route(Record row) {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
