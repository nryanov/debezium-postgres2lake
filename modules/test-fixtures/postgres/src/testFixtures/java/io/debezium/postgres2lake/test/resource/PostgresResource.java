package io.debezium.postgres2lake.test.resource;

import io.debezium.postgres2lake.test.container.PostgresTestContainer;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.HashMap;
import java.util.Map;

public class PostgresResource implements QuarkusTestResourceLifecycleManager {
    public static final String SLOT_NAME_ARG = "slotName";
    public static final String PUBLICATION_NAME_ARG = "publicationName";
    public static final String PREFIX_NAME_ARG = "prefix";
    public static final String CATALOG_TYPE_ARG = "catalog";

    private PostgresHelper postgresHelper;

    private String slotName;
    private String publicationName;
    private String prefix;
    private String catalog;

    @Override
    public void init(Map<String, String> initArgs) {
        this.slotName = initArgs.get(SLOT_NAME_ARG);
        this.publicationName = initArgs.get(PUBLICATION_NAME_ARG);
        this.prefix = initArgs.get(PREFIX_NAME_ARG);
        this.catalog = initArgs.get(CATALOG_TYPE_ARG);

        if (slotName == null || publicationName == null || prefix == null) {
            throw new IllegalArgumentException("prefix, slotName and publicationName should be passed as initArgs in PostgresResource");
        }
    }

    @Override
    public Map<String, String> start() {
        var postgres = PostgresTestContainer.ensureSharedDebeziumStarted();

        postgresHelper = new PostgresHelper(postgres);
        setup();

        var properties = new HashMap<String, String>();
        properties.put("debezium.engine.database.hostname", postgres.getHost());
        properties.put("debezium.engine.database.dbname", postgres.getDatabaseName());
        properties.put("debezium.engine.database.port", String.valueOf(postgres.getMappedPort(5432)));
        properties.put("debezium.engine.database.user", postgres.getUsername());
        properties.put("debezium.engine.database.password", postgres.getPassword());
        properties.put("debezium.engine.slot.name", slotName);
        properties.put("debezium.engine.publication.name", publicationName);
        properties.put("debezium.engine.snapshot.mode", "NO_DATA");
        properties.put("debezium.engine.topic.prefix", prefix);
        // default properties
        properties.put("debezium.engine.name", "test");
        properties.put("debezium.engine.publication.autocreate.mode", "disabled");
        properties.put("debezium.engine.plugin.name", "pgoutput");
        properties.put("debezium.engine.tombstones.on.delete", "false");
        properties.put("debezium.engine.offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");

        switch (catalog) {
            case "iceberg" -> {
                properties.put("debezium.output.iceberg.properties.type", "jdbc");
                properties.put("debezium.output.iceberg.properties.uri", postgres.getJdbcUrl());
                properties.put("debezium.output.iceberg.properties.jdbc.user", postgres.getUsername());
                properties.put("debezium.output.iceberg.properties.jdbc.password", postgres.getPassword());
                properties.put("debezium.output.iceberg.properties.jdbc.schema-version", "V1");
            }
            case "paimon" -> {
                var uri = String.format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", postgres.getHost(), postgres.getMappedPort(5432), postgres.getDatabaseName(), postgres.getUsername(), postgres.getPassword());
                properties.put("debezium.output.paimon.properties.metastore", "jdbc");
                properties.put("debezium.output.paimon.properties.uri", uri);
            }
            case null, default -> {}
        }

        return properties;
    }

    private void setup() {
        postgresHelper.executeSql(PostgresQueries.createLogicalSlot(slotName));
        postgresHelper.executeSql(PostgresQueries.createPublication(publicationName));
    }

    @Override
    public void stop() {
        PostgresTestContainer.stopSharedDebezium();
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(postgresHelper, new TestInjector.AnnotatedAndMatchesType(InjectPostgresHelper.class, PostgresHelper.class));
    }
}
