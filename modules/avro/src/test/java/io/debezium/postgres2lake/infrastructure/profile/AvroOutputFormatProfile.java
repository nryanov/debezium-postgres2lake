package io.debezium.postgres2lake.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class AvroOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.avro.threshold.records", "1",
                "debezium.output.avro.threshold.time", "30s",
                "debezium.output.avro.naming-strategy.partitioner", "UNPARTITIONED",
                "debezium.output.avro.naming-strategy.file-name", "PROCESSING_TIME"
        );
    }
}
