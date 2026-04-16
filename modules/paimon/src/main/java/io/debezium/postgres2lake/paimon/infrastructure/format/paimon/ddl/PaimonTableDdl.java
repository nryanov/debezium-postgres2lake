package io.debezium.postgres2lake.paimon.infrastructure.format.paimon.ddl;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.exceptions.PaimonTableAlterException;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;

import java.util.ArrayList;
import java.util.Map;

public class PaimonTableDdl {
    private final Catalog catalog;
    private final PaimonSchemaConverter converter;

    private final Map<String, String> defaultTableProperties;
    private final Map<String, Map<String, String>> tableProperties;

    public PaimonTableDdl(Catalog catalog, PaimonSchemaConverter converter) {
        this(catalog, converter, Map.of(), Map.of());
    }

    public PaimonTableDdl(
            Catalog catalog,
            PaimonSchemaConverter converter,
            Map<String, String> defaultTableProperties,
            Map<String, Map<String, String>> tableProperties
    ) {
        this.catalog = catalog;
        this.converter = converter;
        this.defaultTableProperties = defaultTableProperties;
        this.tableProperties = tableProperties;
    }

    public Identifier tableIdentifier(EventRecord event) {
        var destination = event.destination();
        // intentionally join db & paimonSchema using `_` instead of pass it as different levels -> not all catalogs support nested schemas
        var schema = String.format("%s_%s", destination.database(), destination.schema());
        return new Identifier(schema, destination.table());
    }

    public void createTableIfNotExists(Identifier identifier, Schema schema) {
        createDatabaseIfNotExists(identifier.getDatabaseName());
        try {
            defaultTableProperties.forEach((key, value) -> schema.options().put(key, value));
            tableProperties.getOrDefault(identifier.getFullName(), Map.of()).forEach((key, value) -> schema.options().put(key, value));
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

    public void handleSchemaEvolution(Identifier identifier, AvroSchemaChanges changes) {
        try {
            var paimonTableChanges = new ArrayList<SchemaChange>();

            for (var change : changes.changes()) {
                switch (change.changeType()) {
                    case ADD -> {
                        var addColumn = (AvroSchemaChanges.AddColumn) change;
                        paimonTableChanges.add(SchemaChange.addColumn(addColumn.columnNameParts(), converter.convertAvroSchema(addColumn.type()), null, null));
                    }
                    case DELETE -> {
                        var deleteColumn = (AvroSchemaChanges.DeleteColumn) change;
                        paimonTableChanges.add(SchemaChange.dropColumn(deleteColumn.columnNameParts()));
                    }
                    case MAKE_OPTIONAL -> {
                        var makeOptional = (AvroSchemaChanges.MakeOptional) change;
                        paimonTableChanges.add(SchemaChange.updateColumnNullability(makeOptional.columnNameParts(), true));
                    }
                    case WIDE -> {
                        var wideColumn = (AvroSchemaChanges.WideColumnType) change;
                        paimonTableChanges.add(SchemaChange.updateColumnType(wideColumn.columnNameParts(), converter.convertAvroSchema(wideColumn.type()), true));
                    }
                }
            }

            catalog.alterTable(identifier, paimonTableChanges, false);
        } catch (Exception e) {
            throw new PaimonTableAlterException(e);
        }
    }
}
