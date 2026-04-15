package io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.ddl;

import io.debezium.postgres2lake.iceberg.config.IcebergConfiguration;
import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.test.avro.AvroTestFixtures;
import io.debezium.postgres2lake.test.container.MinioTestContainer;
import io.debezium.postgres2lake.test.container.PostgresTestContainer;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergTableDdlTest {

    private static final String BUCKET = "ddl-iceberg-bucket";

    private static final PostgreSQLContainer<?> POSTGRES = PostgresTestContainer.newDedicatedForJdbcCatalog();
    private static final MinIOContainer MINIO = MinioTestContainer.newDedicated();

    private static PostgresHelper postgresHelper;
    private static MinioHelper minioHelper;

    @BeforeAll
    static void beforeAll() {
        POSTGRES.start();
        MINIO.start();
        postgresHelper = new PostgresHelper(POSTGRES);
        minioHelper = new MinioHelper(
                MINIO.getS3URL(), MinioTestContainer.ACCESS_KEY, MinioTestContainer.SECRET_KEY);
        minioHelper.createBucket(BUCKET);
    }

    @AfterAll
    static void afterAll() {
        if (MINIO.isRunning()) {
            MINIO.stop();
        }
        if (POSTGRES.isRunning()) {
            POSTGRES.stop();
        }
    }

    private record TestIcebergTableSpec(
            Optional<String> location,
            Map<String, String> properties,
            List<String> partitionBy,
            List<String> sortBy
    ) implements IcebergConfiguration.IcebergTableSpec {}

    private Catalog newCatalog(String warehouseSubdir) {
        var props = new java.util.HashMap<String, String>();

        props.put("type", "jdbc");
        props.put("uri", postgresHelper.jdbcUrl());
        props.put("jdbc.user", postgresHelper.getUsername());
        props.put("jdbc.password", postgresHelper.getPassword());
        props.put("jdbc.schema-version", "V1");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("warehouse", "s3a://" + BUCKET + "/" + warehouseSubdir);
        props.put("s3.endpoint", minioHelper.endpoint());
        props.put("s3.access-key-id", minioHelper.getAccessKey());
        props.put("s3.secret-access-key", minioHelper.getSecretAccessKey());
        props.put("s3.path-style-access", "true");
        props.put("s3.client-factory-impl", "io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");

        Catalog catalog = null;

        while (catalog == null) {
            try {
                catalog = CatalogUtil.buildIcebergCatalog("ddl-test", props, new Configuration());
            } catch (UncheckedSQLException e) {
                // In some cases jdbc catalog may throw this error.
                // ignore
            }
        }

        return catalog;
    }

    @Test
    void tableIdentifierFromEventRecord() {
        var catalog = newCatalog("wh-id-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var keySchema = AvroTestFixtures.record("Key", List.of(AvroTestFixtures.field("id", AvroTestFixtures.required(org.apache.avro.Schema.Type.LONG))));
        var valueSchema = AvroTestFixtures.record("Val", List.of());
        var event = AvroTestFixtures.insertEvent(AvroTestFixtures.emptyRecord(keySchema), AvroTestFixtures.emptyRecord(valueSchema));

        var id = ddl.tableIdentifier(event);

        assertEquals(Namespace.of("db_schema"), id.namespace());
        assertEquals("table", id.name());
    }

    @Test
    void createTableSetsMergeOnReadAndPersistsSchema() {
        var catalog = newCatalog("wh-create-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_create"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())
        );

        var table = ddl.createTableIfNotExists(tableId, schema, Optional.empty());

        assertTrue(catalog.tableExists(tableId));
        assertEquals(RowLevelOperationMode.MERGE_ON_READ.modeName(), table.properties().get(TableProperties.UPDATE_MODE));
        assertEquals(RowLevelOperationMode.MERGE_ON_READ.modeName(), table.properties().get(TableProperties.DELETE_MODE));
        assertNotNull(table.schema().findField("id"));
        assertNotNull(table.schema().findField("name"));
    }

    @Test
    void createTableIfNotExistsIsIdempotent() {
        var catalog = newCatalog("wh-idem-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_idem"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

        var first = ddl.createTableIfNotExists(tableId, schema, Optional.empty());
        var second = ddl.createTableIfNotExists(tableId, schema, Optional.empty());

        assertEquals(first.uuid(), second.uuid());
    }

    @Test
    void handleSchemaEvolutionAddColumn() {
        var catalog = newCatalog("wh-add-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_add"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
        var table = ddl.createTableIfNotExists(tableId, schema, Optional.empty());

        var nickType = AvroTestFixtures.required(org.apache.avro.Schema.Type.STRING);
        ddl.handleSchemaEvolution(table, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.AddColumn(List.of(), "nick", nickType)
        )));

        var reloaded = catalog.loadTable(tableId);
        assertNotNull(reloaded.schema().findField("nick"));
    }

    @Test
    void handleSchemaEvolutionDropColumn() {
        var catalog = newCatalog("wh-drop-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_drop"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "extra", Types.StringType.get())
        );
        var table = ddl.createTableIfNotExists(tableId, schema, Optional.empty());

        ddl.handleSchemaEvolution(table, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.DeleteColumn(List.of(), "extra")
        )));

        var reloaded = catalog.loadTable(tableId);
        assertNull(reloaded.schema().findField("extra"));
    }

    @Test
    void handleSchemaEvolutionMakeOptional() {
        var catalog = newCatalog("wh-opt-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_opt"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "tier", Types.IntegerType.get())
        );
        var table = ddl.createTableIfNotExists(tableId, schema, Optional.empty());
        assertFalse(table.schema().findField("tier").isOptional());

        ddl.handleSchemaEvolution(table, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.MakeOptional(List.of(), "tier")
        )));

        var reloaded = catalog.loadTable(tableId);
        assertTrue(reloaded.schema().findField("tier").isOptional());
    }

    @Test
    void handleSchemaEvolutionWidePrimitiveWidenIntToLong() {
        var catalog = newCatalog("wh-wide-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_wide"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "cnt", Types.IntegerType.get())
        );
        var table = ddl.createTableIfNotExists(tableId, schema, Optional.empty());
        assertInstanceOf(Types.IntegerType.class, table.schema().findField("cnt").type());

        var longAvro = AvroTestFixtures.required(org.apache.avro.Schema.Type.LONG);
        ddl.handleSchemaEvolution(table, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.WideColumnType(List.of(), "cnt", longAvro)
        )));

        var reloaded = catalog.loadTable(tableId);
        assertInstanceOf(Types.LongType.class, reloaded.schema().findField("cnt").type());
    }

    @Test
    void createTableWithPartitionAndSortSpec() {
        var catalog = newCatalog("wh-spec-" + UUID.randomUUID());
        var ddl = new IcebergTableDdl(catalog);
        var tableId = TableIdentifier.of(Namespace.of("ns_spec"), "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "event_ts", Types.TimestampType.withoutZone())
        );
        var spec = new TestIcebergTableSpec(
                Optional.empty(),
                Map.of(),
                List.of("day(event_ts)"),
                List.of("id:asc")
        );

        var table = ddl.createTableIfNotExists(tableId, schema, Optional.of(spec));

        assertEquals(1, table.spec().fields().size());
        assertFalse(table.sortOrder().isUnsorted());
    }
}
