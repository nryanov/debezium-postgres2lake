package io.debezium.postgres2lake.avro.infrastructure.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class ConfluentAvroOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "debezium.avro.format", "CONFLUENT",
                "debezium.output.avro.threshold.records", "1",
                "debezium.output.avro.threshold.time", "30s",
                "debezium.output.avro.naming-strategy.partitioner", "UNPARTITIONED",
                "debezium.output.avro.naming-strategy.file-name", "PROCESSING_TIME",
                "debezium.output.avro.naming-strategy.storage", "S3",
                "debezium.output.avro.naming-strategy.target-path", "warehouse"
        );
    }
}
