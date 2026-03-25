package io.debezium.postgres2lake.test.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;

public class PaimonHelper {
    private final Catalog catalog;

    public PaimonHelper(String warehouse, PostgresHelper postgresHelper, MinioHelper minioHelper) {
        var config = new Configuration();
        config.set("fs.s3a.access.key", minioHelper.getAccessKey());
        config.set("fs.s3a.secret.key", minioHelper.getSecretAccessKey());
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", minioHelper.endpoint());

        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", warehouse);
        options.set("jdbc-url", postgresHelper.jdbcUrl());
        options.set("jdbc-user", postgresHelper.getUsername());
        options.set("jdbc-password", postgresHelper.getPassword());
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        var catalogContext = CatalogContext.create(options, config);
        catalog = CatalogFactory.createCatalog(catalogContext);
    }

    public RecordReader<InternalRow> readTable(String namespace, String table) {
        try {
            var tableIdentifier = Identifier.create(namespace, table);
            var paimonTable = catalog.getTable(tableIdentifier);

            var tableReaderBuilder = paimonTable.newReadBuilder().newRead();
            return tableReaderBuilder.createReader(paimonTable.newReadBuilder().newScan().plan().splits());
        } catch (Catalog.TableNotExistException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
