package io.debezium.postgres2lake.iceberg.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class IcebergOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.iceberg.threshold.records", "1",
                "debezium.output.iceberg.threshold.time", "30s",
                "debezium.output.iceberg.name", "development"
        );
    }
}
