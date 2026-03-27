package io.debezium.postgres2lake.test.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

import java.util.HashMap;

public class IcebergHelper {
    private final Catalog catalog;

    public IcebergHelper(String warehouse, PostgresHelper postgresHelper, MinioHelper minioHelper) {
        var props = new HashMap<String, String>();
        props.put("type", "jdbc");
        props.put("uri", postgresHelper.jdbcUrl());
        props.put("jdbc.user", postgresHelper.getUsername());
        props.put("jdbc.password", postgresHelper.getPassword());
        props.put("jdbc.schema-version", "V1");
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("warehouse", warehouse);
        props.put("s3.endpoint", minioHelper.endpoint());
        props.put("s3.access-key-id", minioHelper.getAccessKey());
        props.put("s3.secret-access-key", minioHelper.getSecretAccessKey());
        props.put("s3.path-style-access", "true");
        props.put("s3.client-factory-impl", "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");

        catalog = CatalogUtil.buildIcebergCatalog("development", props, new Configuration());
    }

    public Table load(String namespace, String table) {
        return catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), table));
    }

    public CloseableIterable<Record> readTable(Table table) {
        return IcebergGenerics.read(table).build();
    }

    public CloseableIterable<Record> readTable(String namespace, String table) {
        var icebergTable = load(namespace, table);
        return readTable(icebergTable);
    }
}
