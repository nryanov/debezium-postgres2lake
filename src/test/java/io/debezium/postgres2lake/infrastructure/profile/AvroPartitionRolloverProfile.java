package io.debezium.postgres2lake.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class AvroPartitionRolloverProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "AVRO",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.avro.naming-strategy.partitioner", "RECORD_FIELD",
                "output.avro.naming-strategy.record-partition-field", "lake_part",
                "output.avro.naming-strategy.file-name", "PROCESSING_TIME");
    }
}
