package io.debezium.postgres2lake.iceberg.infrastructure.s3;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.iceberg.infrastructure.profile.IcebergOutputFormatProfile;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.iceberg.test.helper.IcebergHelper;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectNessieHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.NessieHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.NessieResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.iceberg.data.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "iceberg"),
})
@QuarkusTestResource(value = NessieResource.class, restrictToAnnotatedClass = true)
public class S3IcebergNessieCatalogTest {

    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";
    private static final String NS = "default_public";

    @Inject
    EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    @InjectNessieHelper
    NessieHelper nessieHelper;

    private IcebergHelper icebergHelper;

    @BeforeEach
    void setupIcebergHelper() {
        if (icebergHelper == null) {
            icebergHelper = IcebergHelper.nessie(String.format("s3a://%s", BUCKET), nessieHelper, "main", minioHelper);
        }
    }

    @Test
    void nessieCatalogWritesSmallint() {
        var table = "public.test_smallint_nessie";
        postgresHelper.executeSql(PostgresQueries.createSmallintTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertSmallintRow(table, 1));

        var row = waitAndReadIcebergRecord(NS, "test_smallint_nessie");

        assertEquals(row.getField("required_field"), 1);
        assertEquals(row.getField("optional_field"), 2);
        assertEquals(row.getField("required_array_field"), List.of(1, 2));
        assertEquals(row.getField("optional_array_field"), List.of(3, 4));
    }

    private Record waitAndReadIcebergRecord(String schema, String table) {
        var saver = (AbstractEventSaver<?>) eventSaver;
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> saver.getCurrentRecords() > 0);
        eventSaver.flush();

        var data = icebergHelper.readTable(schema, table).iterator();
        assertTrue(data.hasNext(), "No iceberg data found for table: " + schema + "." + table);
        return data.next();
    }
}
