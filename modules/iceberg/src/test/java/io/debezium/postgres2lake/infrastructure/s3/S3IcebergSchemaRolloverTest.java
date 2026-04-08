package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastructure.profile.IcebergOutputFormatProfile;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.IcebergHelper;
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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(IcebergOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot"),
        @ResourceArg(name = PostgresResource.CATALOG_TYPE_ARG, value = "iceberg")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "iceberg")
})
public class S3IcebergSchemaRolloverTest {

    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";
    private static final String ICEBERG_NAMESPACE = "default_public";

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
    void schemaChangeInOneTransactionCreatesTwoDataFiles() throws IOException {
        var table = "public.test_schema_rollover_iceberg_files";
        postgresHelper.executeSql(SchemaRolloverTestQueries.createMinimalSchemaRolloverTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(SchemaRolloverTestQueries.schemaRolloverTransaction(table));

        var saver = (AbstractEventSaver<?>) eventSaver;
        WriterRolloverAssertions.awaitAndFlush(eventSaver, saver);

        var icebergHelper = IcebergHelper.jdbc(String.format("s3a://%s", BUCKET), postgresHelper, minioHelper);
        var icebergTable = icebergHelper.load(ICEBERG_NAMESPACE, "test_schema_rollover_iceberg_files");
        var dataFileCount = countDataFileTasks(icebergTable);
        assertTrue(dataFileCount >= 2,
                "Expected at least two Iceberg data file scan tasks after schema rollover (found " + dataFileCount + ")");
    }

    @Test
    void schemaChangeExposesNewColumnAcrossRows() throws IOException {
        var table = "public.test_schema_rollover_iceberg_rows";
        postgresHelper.executeSql(SchemaRolloverTestQueries.createMinimalSchemaRolloverTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(SchemaRolloverTestQueries.schemaRolloverTransaction(table));

        var saver = (AbstractEventSaver<?>) eventSaver;
        WriterRolloverAssertions.awaitAndFlush(eventSaver, saver);

        var icebergHelper = IcebergHelper.jdbc(String.format("s3a://%s", BUCKET), postgresHelper, minioHelper);
        var byPk = new HashMap<Long, Record>();

        try (var rows = icebergHelper.readTable(ICEBERG_NAMESPACE, "test_schema_rollover_iceberg_rows")) {
            for (var row : rows) {
                var pk = (Long) row.getField("primary_key");
                byPk.put(pk, row);
            }
        }

        assertEquals(2, byPk.size(), "Expected two CDC rows");

        var first = byPk.get(1L);
        var second = byPk.get(2L);

        assertEquals("first", first.getField("v").toString());
        assertEquals("second", second.getField("v").toString());
        assertNull(first.getField("new_col"), "First row predates new_col; expect null");
        assertEquals("extra", second.getField("new_col").toString());
    }

    private static long countDataFileTasks(org.apache.iceberg.Table table) throws IOException {
        var n = 0L;

        try (var tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                n++;
            }
        }

        return n;
    }
}
