package io.debezium.postgres2lake.core.config;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;
import io.debezium.postgres2lake.domain.model.OutputStorageType;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public interface CommonConfiguration {
    interface FileIO {
        Map<String, String> properties();
    }

    interface OutputNamingStrategy {
        OutputPartitionStrategy partitioner();

        OutputFileNameGenerationStrategy fileName();

        Optional<String> recordPartitionField();

        Optional<OutputStorageType> storage();

        Optional<String> targetPath();
    }

    interface Threshold {
        int records();

        Duration time();
    }
}
