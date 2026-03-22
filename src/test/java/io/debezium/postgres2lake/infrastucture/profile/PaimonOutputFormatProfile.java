package io.debezium.postgres2lake.infrastucture.profile;

import io.quarkus.test.junit.QuarkusTestProfile;

import java.util.Map;

public class PaimonOutputFormatProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
                "output.format", "PAIMON",
                "output.threshold.records", "1",
                "output.threshold.time", "30s",
                "debezium.engine.table.include.list", "public.data",
                "debezium.engine.slot.name", "dbz_it_paimon",
                "debezium.engine.publication.name", "dbz_pub_paimon"
        );
    }
}
