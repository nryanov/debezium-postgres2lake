package io.debezium.postgres2lake.core.config;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;

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
    }

    interface Threshold {
        int records();

        Duration time();
    }
}
