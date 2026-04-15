package io.debezium.postgres2lake.parquet.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class ParquetOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.parquet.threshold.records", "1",
                "debezium.output.parquet.threshold.time", "30s",
                "debezium.output.parquet.naming-strategy.partitioner", "UNPARTITIONED",
                "debezium.output.parquet.naming-strategy.file-name", "PROCESSING_TIME",
                "debezium.output.parquet.naming-strategy.storage", "S3",
                "debezium.output.parquet.naming-strategy.target-path", "warehouse"
        );
    }
}
