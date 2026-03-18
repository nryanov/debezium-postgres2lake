package io.debezium.postgres2lake.infrastructure.format.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;

// just a simple named subclass for AppendOnly logic
public class UnpartitionedAppendOnlyWriter extends UnpartitionedWriter<Record> {
    public UnpartitionedAppendOnlyWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            FileIO io,
            long targetFileSize
    ) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
    }
}
