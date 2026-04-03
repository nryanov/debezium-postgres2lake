package io.debezium.postgres2lake.infrastructure.format.paimon.ddl;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.test.avro.AvroTestFixtures;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaimonTableDdlTest {

    private static final String BUCKET = "ddl-paimon-bucket";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "password";

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");

    private static final MinIOContainer MINIO = new MinIOContainer(
            DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z").asCompatibleSubstituteFor("minio"))
            .withUserName(ACCESS_KEY)
            .withPassword(SECRET_KEY);

    private static PostgresHelper postgresHelper;
    private static MinioHelper minioHelper;

    @BeforeAll
    static void beforeAll() {
        POSTGRES.start();
        MINIO.start();
        postgresHelper = new PostgresHelper(POSTGRES);
        minioHelper = new MinioHelper(MINIO.getS3URL(), ACCESS_KEY, SECRET_KEY);
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

    private Catalog newCatalog(String warehouseSubdir) {
        var config = new Configuration();
        config.set("fs.s3a.access.key", minioHelper.getAccessKey());
        config.set("fs.s3a.secret.key", minioHelper.getSecretAccessKey());
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", minioHelper.endpoint());

        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", "s3a://" + BUCKET + "/" + warehouseSubdir);
        options.set("jdbc-url", postgresHelper.jdbcUrl());
        options.set("jdbc-user", postgresHelper.getUsername());
        options.set("jdbc-password", postgresHelper.getPassword());
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        return CatalogFactory.createCatalog(CatalogContext.create(options, config));
    }

    @Test
    void tableIdentifierFromEventRecord() {
        var catalog = newCatalog("wh-id-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var keySchema = AvroTestFixtures.record("Key", List.of(AvroTestFixtures.field("id", AvroTestFixtures.required(org.apache.avro.Schema.Type.LONG))));
        var valueSchema = AvroTestFixtures.record("Val", List.of());
        var event = AvroTestFixtures.insertEvent(AvroTestFixtures.emptyRecord(keySchema), AvroTestFixtures.emptyRecord(valueSchema));

        var id = ddl.tableIdentifier(event);

        assertEquals("db_schema", id.getDatabaseName());
        assertEquals("table", id.getObjectName());
    }

    @Test
    void createTableIfNotExistsPersistsTable() throws Exception {
        var catalog = newCatalog("wh-create-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var id = new Identifier("ns_paimon_create", "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .build();

        ddl.createTableIfNotExists(id, schema);

        var table = catalog.getTable(id);
        assertNotNull(table);
        assertNotNull(table.rowType().getField("id"));
        assertNotNull(table.rowType().getField("name"));
    }

    @Test
    void handleSchemaEvolutionAddColumn() throws Exception {
        var catalog = newCatalog("wh-add-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var id = new Identifier("ns_paimon_add", "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .primaryKey("id")
                .build();
        ddl.createTableIfNotExists(id, schema);

        var nickType = AvroTestFixtures.required(org.apache.avro.Schema.Type.STRING);
        ddl.handleSchemaEvolution(id, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.AddColumn(List.of(), "nick", nickType)
        )));

        var table = catalog.getTable(id);
        assertNotNull(table.rowType().getField("nick"));
    }

    @Test
    void handleSchemaEvolutionDropColumn() throws Exception {
        var catalog = newCatalog("wh-drop-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var id = new Identifier("ns_paimon_drop", "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("extra", DataTypes.STRING())
                .primaryKey("id")
                .build();
        ddl.createTableIfNotExists(id, schema);

        ddl.handleSchemaEvolution(id, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.DeleteColumn(List.of(), "extra")
        )));

        var table = catalog.getTable(id);
        assertTrue(table.rowType().getFields().stream().noneMatch(f -> f.name().equals("extra")));
    }

    @Test
    void handleSchemaEvolutionMakeOptional() throws Exception {
        var catalog = newCatalog("wh-opt-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var id = new Identifier("ns_paimon_opt", "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("tier", DataTypes.INT().notNull())
                .primaryKey("id")
                .build();
        ddl.createTableIfNotExists(id, schema);

        assertFalse(catalog.getTable(id).rowType().getField("tier").type().isNullable());

        ddl.handleSchemaEvolution(id, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.MakeOptional(List.of(), "tier")
        )));

        assertTrue(catalog.getTable(id).rowType().getField("tier").type().isNullable());
    }

    @Test
    void handleSchemaEvolutionWidePrimitiveWidenIntToLong() throws Exception {
        var catalog = newCatalog("wh-wide-" + UUID.randomUUID());
        var ddl = new PaimonTableDdl(catalog, new PaimonSchemaConverter());
        var id = new Identifier("ns_paimon_wide", "t_" + UUID.randomUUID().toString().replace("-", ""));
        var schema = Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("cnt", DataTypes.INT().notNull())
                .primaryKey("id")
                .build();
        ddl.createTableIfNotExists(id, schema);

        assertTrue(catalog.getTable(id).rowType().getField("cnt").type().is(DataTypeRoot.INTEGER));

        var longAvro = AvroTestFixtures.required(org.apache.avro.Schema.Type.LONG);
        ddl.handleSchemaEvolution(id, new AvroSchemaChanges(List.of(
                new AvroSchemaChanges.WideColumnType(List.of(), "cnt", longAvro)
        )));

        assertTrue(catalog.getTable(id).rowType().getField("cnt").type().is(DataTypeRoot.BIGINT));
    }
}
