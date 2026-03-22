package io.debezium.postgres2lake.infrastucture.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class OrcOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "ORC",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "debezium.engine.table.include.list", "public.data",
                "debezium.engine.slot.name", "dbz_it_orc",
                "debezium.engine.publication.name", "dbz_pub_orc"
        );
    }
}
