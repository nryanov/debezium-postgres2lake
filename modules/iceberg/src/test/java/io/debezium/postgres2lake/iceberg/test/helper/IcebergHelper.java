package io.debezium.postgres2lake.iceberg.test.helper;

import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.NessieHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
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
import java.util.Map;

public class IcebergHelper {
    private static final String CATALOG_NAME = "development";

    private final Catalog catalog;

    private IcebergHelper(Map<String, String> catalogProperties, Configuration hadoopConfiguration) {
        this.catalog = CatalogUtil.buildIcebergCatalog(CATALOG_NAME, new HashMap<>(catalogProperties), hadoopConfiguration);
    }

    public static IcebergHelper jdbc(String warehouse, PostgresHelper postgresHelper, MinioHelper minioHelper) {
        var props = new HashMap<String, String>();
        props.put("type", "jdbc");
        props.put("uri", postgresHelper.jdbcUrl());
        props.put("jdbc.user", postgresHelper.getUsername());
        props.put("jdbc.password", postgresHelper.getPassword());
        props.put("jdbc.schema-version", "V1");
        putS3FileIoCatalogProperties(warehouse, minioHelper, props);

        return new IcebergHelper(props, s3aHadoopConfiguration(minioHelper));
    }

    public static IcebergHelper nessie(String warehouse, NessieHelper nessie, String ref, MinioHelper minioHelper) {
        var props = new HashMap<String, String>();
        props.put("type", "nessie");
        props.put("uri", nessie.apiBaseUrl());
        props.put("ref", ref);
        putS3FileIoCatalogProperties(warehouse, minioHelper, props);
        return new IcebergHelper(props, s3aHadoopConfiguration(minioHelper));
    }

    public static IcebergHelper hive(String warehouse, String thriftUri, MinioHelper minioHelper) {
        var props = new HashMap<String, String>();
        props.put("type", "hive");
        props.put("uri", thriftUri);
        putS3FileIoCatalogProperties(warehouse, minioHelper, props);
        return new IcebergHelper(props, s3aHadoopConfiguration(minioHelper));
    }

    public static IcebergHelper hadoop(String warehouse, MinioHelper minioHelper) {
        var props = new HashMap<String, String>();
        props.put("type", "hadoop");
        props.put("warehouse", warehouse);
        putS3FileIoCatalogProperties(warehouse, minioHelper, props);
        return new IcebergHelper(props, s3aHadoopConfiguration(minioHelper));
    }

    private static void putS3FileIoCatalogProperties(String warehouse, MinioHelper minioHelper, Map<String, String> props) {
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("warehouse", warehouse);
        props.put("s3.endpoint", minioHelper.endpoint());
        props.put("s3.access-key-id", minioHelper.getAccessKey());
        props.put("s3.secret-access-key", minioHelper.getSecretAccessKey());
        props.put("s3.path-style-access", "true");
        props.put("s3.client-factory-impl", "io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");
    }

    private static Configuration s3aHadoopConfiguration(MinioHelper minioHelper) {
        var conf = new Configuration();
        conf.set("fs.s3a.access.key", minioHelper.getAccessKey());
        conf.set("fs.s3a.secret.key", minioHelper.getSecretAccessKey());
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.endpoint", minioHelper.endpoint());
        return conf;
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
