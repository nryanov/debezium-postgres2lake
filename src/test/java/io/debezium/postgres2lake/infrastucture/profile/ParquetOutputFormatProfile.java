package io.debezium.postgres2lake.infrastucture.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class ParquetOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "PARQUET",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.parquet.naming-strategy.partitioner", "UNPARTITIONED",
                "output.parquet.naming-strategy.file-name", "PROCESSING_TIME"
        );
    }
}
