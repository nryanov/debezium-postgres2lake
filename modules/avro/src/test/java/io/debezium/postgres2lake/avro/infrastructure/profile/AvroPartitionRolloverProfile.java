package io.debezium.postgres2lake.avro.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class AvroPartitionRolloverProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "BINARY",
                "debezium.output.avro.threshold.records", "1",
                "debezium.output.avro.threshold.time", "30s",
                "debezium.output.avro.naming-strategy.partitioner", "RECORD_FIELD",
                "debezium.output.avro.naming-strategy.record-partition-field", "lake_part",
                "debezium.output.avro.naming-strategy.file-name", "PROCESSING_TIME");
    }
}
