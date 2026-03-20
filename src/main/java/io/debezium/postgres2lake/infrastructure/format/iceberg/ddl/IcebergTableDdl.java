package io.debezium.postgres2lake.infrastructure.format.iceberg.ddl;

import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Optional;

public class IcebergTableDdl {
    private final Catalog catalog;
    private final SupportsNamespaces namespaces;

    public IcebergTableDdl(Catalog catalog) {
        this.catalog = catalog;
        this.namespaces = (SupportsNamespaces) catalog;
    }

    public void createTableIfNotExists(
            TableIdentifier tableIdentifier,
            Schema schema,
            Optional<OutputConfiguration.IcebergTableSpec> maybeTableSpec
    ) {
        Table table;
        createNamespaceIfNotExists(tableIdentifier);

        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
        } else {
            if (maybeTableSpec.isEmpty()) {
                table = catalog.createTable(tableIdentifier, schema);
            } else {
                var tableBuilder = catalog.buildTable(tableIdentifier, schema);

                var tableSpec = maybeTableSpec.get();
                tableSpec.location().ifPresent(tableBuilder::withLocation);

                // set default update.mode & delete.mode
                tableSpec.properties().computeIfAbsent(TableProperties.UPDATE_MODE, key -> RowLevelOperationMode.MERGE_ON_READ.modeName());
                tableSpec.properties().computeIfAbsent(TableProperties.DELETE_MODE, key -> RowLevelOperationMode.MERGE_ON_READ.modeName());
                tableSpec.properties().forEach(tableBuilder::withProperty);
                table = tableBuilder.create();
            }
        }

        validateTableProperties(table);
    }

    private void createNamespaceIfNotExists(TableIdentifier table) {
        if (namespaces.namespaceExists(table.namespace())) {
            return;
        }

        namespaces.createNamespace(table.namespace());
    }

    private void validateTableProperties(Table table) {
        var properties = table.properties();

        var updateMode = properties.get(TableProperties.UPDATE_MODE);
        var deleteMode = properties.get(TableProperties.DELETE_MODE);

        if (!RowLevelOperationMode.MERGE_ON_READ.modeName().equals(updateMode)) {
            throw new IllegalStateException("Table update mode is copy-on-write, but expected merge-on-read");
        }

        if (!RowLevelOperationMode.MERGE_ON_READ.modeName().equals(deleteMode)) {
            throw new IllegalStateException("Table delete mode is copy-on-write, but expected merge-on-read");
        }
    }
}
