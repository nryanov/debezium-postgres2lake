package io.debezium.postgres2lake.core.service;

import io.debezium.postgres2lake.domain.EventFileNameGenerator;
import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.domain.model.OutputStorageType;

public class OutputLocationGenerator {
    private final EventPartitioner partitioner;
    private final EventFileNameGenerator fileNameGenerator;
    private final OutputFileFormat fileFormat;
    private final String targetPath;
    private final OutputStorageType storageType;

    public OutputLocationGenerator(
            EventPartitioner partitioner,
            EventFileNameGenerator fileNameGenerator,
            OutputFileFormat fileFormat,
            String targetPath,
            OutputStorageType storageType
    ) {
        this.partitioner = partitioner;
        this.fileNameGenerator = fileNameGenerator;
        this.fileFormat = fileFormat;
        this.targetPath = targetPath;
        this.storageType = storageType;
    }

    public String getPartition(EventRecord record) {
        return partitioner.resolvePartition(resolveRootPath(), record);
    }

    public String generateLocation(EventRecord record) {
        var partition = partitioner.resolvePartition(resolveRootPath(), record);
        return fileNameGenerator.generate(partition, fileFormat);
    }

    private String resolveRootPath() {
        return switch (storageType) {
            case S3 -> "s3a://" + trimSlashes(targetPath);
            case HDFS -> normalizeHdfsPath(targetPath);
        };
    }

    private static String trimSlashes(String path) {
        return path.replaceAll("^/+", "").replaceAll("/+$", "");
    }

    private static String normalizeHdfsPath(String rawPath) {
        var path = rawPath.trim();
        if (path.startsWith("hdfs://")) {
            return path.replaceAll("/+$", "");
        }

        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        return path.replaceAll("/+$", "");
    }
}
