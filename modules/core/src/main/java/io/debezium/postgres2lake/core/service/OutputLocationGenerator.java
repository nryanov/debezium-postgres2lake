package io.debezium.postgres2lake.core.service;

import io.debezium.postgres2lake.domain.EventFileNameGenerator;
import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;

public class OutputLocationGenerator {
    private final EventPartitioner partitioner;
    private final EventFileNameGenerator fileNameGenerator;
    private final OutputFileFormat fileFormat;

    public OutputLocationGenerator(EventPartitioner partitioner, EventFileNameGenerator fileNameGenerator, OutputFileFormat fileFormat) {
        this.partitioner = partitioner;
        this.fileNameGenerator = fileNameGenerator;
        this.fileFormat = fileFormat;
    }

    public String getPartition(String bucket, EventRecord record) {
        return partitioner.resolvePartition(bucket, record);
    }

    public String generateLocation(String bucket, EventRecord record) {
        var partition = partitioner.resolvePartition(bucket, record);
        return fileNameGenerator.generate(partition, fileFormat);
    }
}
