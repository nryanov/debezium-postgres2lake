package io.debezium.postgres2lake.test;

import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class PostgresResource implements QuarkusTestResourceLifecycleManager {
    public static final String SLOT_NAME_ARG = "slotName";
    public static final String PUBLICATION_NAME_ARG = "publicationName";
    public static final String PREFIX_NAME_ARG = "prefix";

    private static PostgreSQLContainer<?> postgres;
    private PostgresHelper postgresHelper;

    private String slotName;
    private String publicationName;
    private String prefix;

    @Override
    public void init(Map<String, String> initArgs) {
        this.slotName = initArgs.get(SLOT_NAME_ARG);
        this.publicationName = initArgs.get(PUBLICATION_NAME_ARG);
        this.prefix = initArgs.get(PREFIX_NAME_ARG);

        if (slotName == null || publicationName == null || prefix == null) {
            throw new IllegalArgumentException("prefix, slotName and publicationName should be passed as initArgs in PostgresResource");
        }
    }

    @Override
    public Map<String, String> start() {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCommand(
                        "postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_wal_senders=5",
                        "-c", "max_replication_slots=5"
                );
        postgres.start();

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

        return properties;
    }

    private void setup() {
        postgresHelper.executeSql(PostgresQueries.createLogicalSlot(slotName));
        postgresHelper.executeSql(PostgresQueries.createPublication(publicationName));
    }

    @Override
    public void stop() {
        postgres.stop();
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(postgresHelper, new TestInjector.AnnotatedAndMatchesType(InjectPostgresHelper.class, PostgresHelper.class));
    }
}
