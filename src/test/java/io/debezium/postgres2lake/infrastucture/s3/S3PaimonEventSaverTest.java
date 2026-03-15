package io.debezium.postgres2lake.infrastucture.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class S3PaimonEventSaverTest {
    private static Catalog catalog;

    @BeforeAll
    public static void setup() {
        var config = new Configuration();
        config.set("fs.s3a.access.key", "admin");
        config.set("fs.s3a.secret.key", "password");
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", "http://localhost:9000");

        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", "s3a://warehouse/paimon-warehouse");
        options.set("jdbc-url", "jdbc:postgresql://localhost:5432/postgres");
        options.set("jdbc-user", "postgres");
        options.set("jdbc-password", "postgres");
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        var catalogContext = CatalogContext.create(options, config);
        catalog = CatalogFactory.createCatalog(catalogContext);
    }

    @Test
    public void successfullyReadPaimonTable() throws Catalog.TableNotExistException, IOException {
        var tableIdentifier = Identifier.create("paimon-development", "data");
        var table = catalog.getTable(tableIdentifier);

        var tableReaderBuilder = table.newReadBuilder().newRead();
        var reader = tableReaderBuilder.createReader(table.newReadBuilder().newScan().plan().splits());

        var pk = table.rowType().getField("primary_key");
        table.rowType().getFields().forEach(field -> System.out.println(String.format("Field: %s, fieldId: %s", field.name(), field.id())));

        reader.forEachRemaining(row -> {
            var id = row.getLong(pk.id());
            System.out.println(String.format("PK (%d): %d", pk.id(), id));
        });

        reader.close();
    }
}
