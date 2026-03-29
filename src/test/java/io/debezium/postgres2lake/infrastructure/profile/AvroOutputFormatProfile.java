package io.debezium.postgres2lake.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class AvroOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "AVRO",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.avro.naming-strategy.partitioner", "UNPARTITIONED",
                "output.avro.naming-strategy.file-name", "PROCESSING_TIME"
        );
    }
}
