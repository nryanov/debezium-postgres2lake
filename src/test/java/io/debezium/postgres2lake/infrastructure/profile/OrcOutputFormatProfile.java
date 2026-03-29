package io.debezium.postgres2lake.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class OrcOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "ORC",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.orc.naming-strategy.partitioner", "UNPARTITIONED",
                "output.orc.naming-strategy.file-name", "PROCESSING_TIME"
        );
    }
}
