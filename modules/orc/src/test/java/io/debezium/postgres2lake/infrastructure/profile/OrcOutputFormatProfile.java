package io.debezium.postgres2lake.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class OrcOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.orc.threshold.records", "1",
                "debezium.output.orc.threshold.time", "30s",
                "debezium.output.orc.naming-strategy.partitioner", "UNPARTITIONED",
                "debezium.output.orc.naming-strategy.file-name", "PROCESSING_TIME"
        );
    }
}
