package io.debezium.postgres2lake.config;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface CommonConfiguration {
    Threshold threshold();

    interface Avro {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<String> codec();
    }

    interface Parquet {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<String> codec();
    }

    interface Orc {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<String> codec();
    }

    interface Iceberg {
        String name();

        Map<String, String> properties();

        FileIO fileIO();

        Map<String, IcebergTableSpec> tableSpecs();
    }

    interface Paimon {
        Map<String, String> properties();

        FileIO fileIO();
    }

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

    interface IcebergTableSpec {
        Optional<String> location();

        Map<String, String> properties();

        List<String> partitionBy();

        List<String> sortBy();
    }
}
