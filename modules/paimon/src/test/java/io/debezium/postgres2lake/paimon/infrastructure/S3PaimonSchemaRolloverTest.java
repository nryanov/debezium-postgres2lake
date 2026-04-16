package io.debezium.postgres2lake.paimon.infrastructure;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.paimon.infrastructure.profile.PaimonOutputFormatProfile;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.paimon.test.helper.PaimonHelper;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.helper.SchemaRolloverTestQueries;
import io.debezium.postgres2lake.test.helper.WriterRolloverAssertions;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(PaimonOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot"),
        @ResourceArg(name = PostgresResource.CATALOG_TYPE_ARG, value = "paimon")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "paimon")
})
public class S3PaimonSchemaRolloverTest {

    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";
    private static final String PAIMON_NAMESPACE = "default_public";

    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    @BeforeEach
    void flushPending() {
        eventSaver.flush();
    }

    @Test
    void schemaChangeInOneTransactionCreatesAtLeastTwoParquetFiles() {
        var logicalTable = "test_schema_rollover_paimon_files";
        var pgTable = "public." + logicalTable;
        postgresHelper.executeSql(SchemaRolloverTestQueries.createMinimalSchemaRolloverTable(pgTable));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, pgTable));
        postgresHelper.executeSql(SchemaRolloverTestQueries.schemaRolloverTransaction(pgTable));

        var saver = (AbstractEventSaver<?>) eventSaver;
        WriterRolloverAssertions.awaitAndFlush(eventSaver, saver);

        var parquetCount = countParquetObjectsForTable(logicalTable);
        assertTrue(parquetCount >= 2,
                "Expected at least two Parquet objects for table after schema rollover (found " + parquetCount + ")");
    }

    @Test
    void schemaChangeExposesNewColumnAcrossRows() throws Exception {
        var logicalTable = "test_schema_rollover_paimon_rows";
        var pgTable = "public." + logicalTable;
        postgresHelper.executeSql(SchemaRolloverTestQueries.createMinimalSchemaRolloverTable(pgTable));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, pgTable));
        postgresHelper.executeSql(SchemaRolloverTestQueries.schemaRolloverTransaction(pgTable));

        var saver = (AbstractEventSaver<?>) eventSaver;
        WriterRolloverAssertions.awaitAndFlush(eventSaver, saver);

        var paimonHelper = PaimonHelper.forJdbc(String.format("s3a://%s", BUCKET), postgresHelper, minioHelper);
        var data = paimonHelper.readTable(PAIMON_NAMESPACE, logicalTable);

        var byPk = new HashMap<Long, Map<String, Object>>();
        try (var it = data.iterator()) {
            while (it.hasNext()) {
                var row = PaimonHelper.readRowAsMap(data.fields(), it.next());
                var pk = (Long) row.get("primary_key");
                byPk.put(pk, row);
            }
        }

        assertEquals(2, byPk.size(), "Expected two CDC rows");

        var first = byPk.get(1L);
        var second = byPk.get(2L);
        assertEquals("first", first.get("v").toString());
        assertEquals("second", second.get("v").toString());
        assertNull(first.get("new_col"), "First row predates new_col; expect null");
        assertEquals("extra", second.get("new_col").toString());
    }

    private long countParquetObjectsForTable(String logicalTableName) {
        return minioHelper.listObjectKeys(BUCKET, "").stream()
                .filter(key -> key.contains(logicalTableName) && key.endsWith(".parquet"))
                .count();
    }
}
