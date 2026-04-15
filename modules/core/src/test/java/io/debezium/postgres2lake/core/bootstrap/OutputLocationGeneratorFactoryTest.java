package io.debezium.postgres2lake.core.bootstrap;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;
import io.debezium.postgres2lake.domain.model.OutputStorageType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OutputLocationGeneratorFactoryTest {

    @Test
    void resolveUsesConfiguredS3StorageAndTargetPath() {
        var strategy = new NamingStrategy(
                OutputPartitionStrategy.UNPARTITIONED,
                OutputFileNameGenerationStrategy.UUID,
                Optional.empty(),
                OutputStorageType.S3,
                "warehouse"
        );

        var generator = OutputLocationGeneratorFactory.resolve(strategy, OutputFileFormat.avro);
        var record = testRecord();

        var partition = generator.getPartition(record);
        var location = generator.generateLocation(record);

        assertEquals("s3a://warehouse/db/schema/table", partition);
        assertTrue(location.startsWith("s3a://warehouse/db/schema/table/"));
        assertTrue(location.endsWith(".avro"));
    }

    @Test
    void resolveUsesConfiguredHdfsTargetPath() {
        var strategy = new NamingStrategy(
                OutputPartitionStrategy.UNPARTITIONED,
                OutputFileNameGenerationStrategy.UUID,
                Optional.empty(),
                OutputStorageType.HDFS,
                "local://tmp/root/"
        );

        var generator = OutputLocationGeneratorFactory.resolve(strategy, OutputFileFormat.parquet);

        assertEquals("local://tmp/root/db/schema/table", generator.getPartition(testRecord()));
    }

    private static EventRecord testRecord() {
        var emptyRecord = new GenericData.Record(Schema.createRecord("value", null, null, false, List.of()));
        return new EventRecord(Operation.INSERT, emptyRecord, emptyRecord, "db.schema.table");
    }

    private record NamingStrategy(
            OutputPartitionStrategy partitioner,
            OutputFileNameGenerationStrategy fileName,
            Optional<String> recordPartitionField,
            OutputStorageType storage,
            String targetPath
    ) implements CommonConfiguration.OutputNamingStrategy {
    }
}
