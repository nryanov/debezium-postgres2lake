package io.debezium.postgres2lake.infrastructure.format.paimon.ddl;

import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;

public class PaimonTableDdl {
    private final Catalog catalog;

    public PaimonTableDdl(Catalog catalog) {
        this.catalog = catalog;
    }

    public Identifier tableIdentifier(EventRecord event) {
        var destination = event.destination();
        // intentionally join db & schema using `_` instead of pass it as different levels -> not all catalogs support nested schemas
        var schema = String.format("%s_%s", destination.database(), destination.schema());
        return new Identifier(schema, destination.table());
    }

    public void createTableIfNotExists(Identifier identifier, Schema schema) {
        createDatabaseIfNotExists(identifier.getDatabaseName());
        try {
            catalog.createTable(identifier, schema, true);
        } catch (Catalog.TableAlreadyExistException | Catalog.DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDatabaseIfNotExists(String schema) {
        try {
            catalog.createDatabase(schema, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new RuntimeException(e);
        }
    }
}
