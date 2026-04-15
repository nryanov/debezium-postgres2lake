package io.debezium.postgres2lake.orc.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class OrcPartitionRolloverProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.orc.threshold.records", "1",
                "debezium.output.orc.threshold.time", "30s",
                "debezium.output.orc.naming-strategy.partitioner", "RECORD_FIELD",
                "debezium.output.orc.naming-strategy.record-partition-field", "lake_part",
                "debezium.output.orc.naming-strategy.file-name", "PROCESSING_TIME",
                "debezium.output.orc.naming-strategy.storage", "S3",
                "debezium.output.orc.naming-strategy.target-path", "warehouse");
    }
}
