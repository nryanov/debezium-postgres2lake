package io.debezium.postgres2lake.infrastucture.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class IcebergOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "ICEBERG",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "output.iceberg.name", "development"
        );
    }
}
