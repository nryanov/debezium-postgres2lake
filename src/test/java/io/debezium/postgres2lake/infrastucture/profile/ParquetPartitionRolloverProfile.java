package io.debezium.postgres2lake.infrastucture.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class ParquetPartitionRolloverProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "PARQUET",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.parquet.naming-strategy.partitioner", "RECORD_FIELD",
                "output.parquet.naming-strategy.record-partition-field", "lake_part",
                "output.parquet.naming-strategy.file-name", "PROCESSING_TIME");
    }
}
