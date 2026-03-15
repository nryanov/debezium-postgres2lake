package io.debezium.postgres2lake.infrastucture.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

public class S3IcebergEventSaverTest {
    private static Catalog catalog;

    @BeforeAll
    public static void setup() {
        var props = new HashMap<String, String>();
        props.put("type", "jdbc");
        props.put("uri", "jdbc:postgresql://localhost:5432/postgres");
        props.put("jdbc.user", "postgres");
        props.put("jdbc.password", "postgres");
        props.put("jdbc.schema-version", "V1");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("warehouse", "s3a://warehouse");
        props.put("s3.endpoint", "http://localhost:9000");
        props.put("s3.access-key-id", "admin");
        props.put("s3.secret-access-key", "password");
        props.put("s3.path-style-access", "true");
        props.put("s3.client-factory-impl", "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");

        catalog = CatalogUtil.buildIcebergCatalog("development", props, new Configuration());
    }

    @Test
    public void successfullyReadIcebergTable() {
        var table = catalog.loadTable(TableIdentifier.of(Namespace.of("development"), "data"));

        System.out.println("Reading records from the table:");
        try (var result = IcebergGenerics.read(table).build()) {
            for (var record : result) {
                System.out.println(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
