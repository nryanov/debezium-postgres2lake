package io.debezium.postgres2lake.infrastructure.format.iceberg.ddl;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class IcebergTableDdl {
    private record Tuple(String field, String option) {}

    private static final Pattern PARTITION_BY_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");
    private static final Pattern SORT_BY_REGEX = Pattern.compile("(\\w+):(asc|desc)");

    private static final Map<String, String> DEFAULT_TABLE_PROPERTIES = Map.of(
            TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName(),
            TableProperties.DELETE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName()
    );

    private final Catalog catalog;
    private final SupportsNamespaces namespaces;

    public IcebergTableDdl(Catalog catalog) {
        this.catalog = catalog;
        this.namespaces = (SupportsNamespaces) catalog;
    }

    public TableIdentifier tableIdentifier(EventRecord event) {
        var destination = event.destination();
        // intentionally join db & paimonSchema using `_` instead of pass it as different levels -> not all catalogs support nested schemas
        var namespace = Namespace.of(String.format("%s_%s", destination.database(), destination.schema()));
        return TableIdentifier.of(namespace, destination.table());
    }

    public Table createTableIfNotExists(
            TableIdentifier tableIdentifier,
            Schema schema,
            Optional<OutputConfiguration.IcebergTableSpec> maybeTableSpec
    ) {
        Table table;
        createNamespaceIfNotExists(tableIdentifier);

        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
        } else {
            var tableBuilder = catalog.buildTable(tableIdentifier, schema);
            if (maybeTableSpec.isEmpty()) {
                tableBuilder.withSortOrder(SortOrder.unsorted());
                tableBuilder.withPartitionSpec(PartitionSpec.unpartitioned());
                tableBuilder.withProperties(DEFAULT_TABLE_PROPERTIES);
            } else {
                var tableSpec = maybeTableSpec.get();
                tableSpec.location().ifPresent(tableBuilder::withLocation);
                // set default update.mode & delete.mode
                tableSpec.properties().computeIfAbsent(TableProperties.UPDATE_MODE, key -> RowLevelOperationMode.MERGE_ON_READ.modeName());
                tableSpec.properties().computeIfAbsent(TableProperties.DELETE_MODE, key -> RowLevelOperationMode.MERGE_ON_READ.modeName());
                tableSpec.properties().forEach(tableBuilder::withProperty);

                tableBuilder.withSortOrder(resolveSortOrder(schema, tableSpec.sortBy()));
                tableBuilder.withPartitionSpec(resolvePartitionSpec(schema, tableSpec.partitionBy()));
            }

            table = tableBuilder.create();
        }

        validateTableProperties(table);
        return table;
    }

    private SortOrder resolveSortOrder(Schema schema, List<String> sortBy) {
        if (sortBy.isEmpty()) {
            return SortOrder.unsorted();
        }

        var builder = SortOrder.builderFor(schema);

        sortBy.forEach(it -> {
            var matcher = SORT_BY_REGEX.matcher(it);
            if (matcher.matches()) {
                var field = matcher.group(1);
                var direction = SortDirection.fromString(matcher.group(2));
                builder.sortBy(field, direction, NullOrder.NULLS_LAST);
            }
        });

        return builder.build();
    }

    private PartitionSpec resolvePartitionSpec(Schema schema, List<String> partitionBy) {
        if (partitionBy.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }

        var builder = PartitionSpec.builderFor(schema);

        partitionBy.forEach(it -> {
            var matcher = PARTITION_BY_REGEX.matcher(it);
            if (matcher.matches()) {
                var transform = matcher.group(1);
                switch (transform) {
                    case "hour" -> {
                        var field = matcher.group(2);
                        builder.hour(field);
                    }
                    case "day" -> {
                        var field = matcher.group(2);
                        builder.day(field);
                    }
                    case "month" -> {
                        var field = matcher.group(2);
                        builder.month(field);
                    }
                    case "year" -> {
                        var field = matcher.group(2);
                        builder.year(field);
                    }
                    case "truncate" -> {
                        var tuple = resolveTransformSettings(matcher.group(2));
                        builder.truncate(tuple.field, Integer.parseInt(tuple.option));
                    }
                    case "bucket" -> {
                        var tuple = resolveTransformSettings(matcher.group(2));
                        builder.bucket(tuple.field, Integer.parseInt(tuple.option));
                    }
                    default -> throw new IllegalArgumentException("Unsupported partition transform: " + it);
                }
            } else {
                builder.identity(it);
            }
        });

        return builder.build();
    }

    private static Tuple resolveTransformSettings(String settings) {
        var parts = settings.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Transform should has exactly 2 arguments, but got this: " + settings);
        }

        var field = parts[0].trim();
        var option = parts[1].trim();

        return new Tuple(field, option);
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

    public void handleSchemaEvolution(Table table, AvroSchemaChanges changes) {
        var idx = new AtomicInteger(0);
        var updates = table.updateSchema();

        for (var change : changes.changes()) {
            switch (change.changeType()) {
                case ADD -> {
                    var addColumn = (AvroSchemaChanges.AddColumn) change;
                    updates.addColumn(addColumn.parentColumnName(), addColumn.name(), resolveAvroType(idx, addColumn.type()));
                }
                case DELETE -> {
                    var deleteColumn = (AvroSchemaChanges.DeleteColumn) change;
                    updates.deleteColumn(deleteColumn.fullColumnName());
                }
                case MAKE_OPTIONAL -> {
                    var makeOptional = (AvroSchemaChanges.MakeOptional) change;
                    updates.makeColumnOptional(makeOptional.fullColumnName());
                }
                case WIDE -> {
                    var wideColumn = (AvroSchemaChanges.WideColumnType) change;
                    updates.updateColumn(wideColumn.fullColumnName(), resolveAvroType(idx, wideColumn.type()).asPrimitiveType());
                }
            }
        }

        updates.commit();
    }

    private Types.NestedField resolveField(AtomicInteger idx, org.apache.avro.Schema.Field field) {
        var schema = field.schema();
        var isOptional = schema.isNullable();
        var name = field.name();

        return Types.NestedField.builder()
                .isOptional(isOptional)
                .withName(name)
                .withId(idx.incrementAndGet())
                .ofType(resolveAvroType(idx, schema))
                .build();
    }

    private Type resolveAvroType(AtomicInteger idx, org.apache.avro.Schema schema) {
        var logicalType = schema.getLogicalType();
        if (logicalType != null) {
            return resolveAvroLogicalType(logicalType, schema);
        }

        return switch (schema.getType()) {
            case BOOLEAN -> Types.BooleanType.get();
            case INT -> Types.IntegerType.get();
            case LONG -> Types.LongType.get();
            case BYTES -> Types.BinaryType.get();
            case UNION -> resolveAvroType(idx, schema.getTypes().get(1));
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case STRING, ENUM, FIXED -> Types.StringType.get();
            case MAP -> {
                var valueType = schema.getValueType();
                var isOptional = valueType.isNullable();

                if (isOptional) {
                    yield Types.MapType.ofOptional(
                            idx.getAndIncrement(),
                            idx.getAndIncrement(),
                            Types.StringType.get(),
                            resolveAvroType(idx, valueType));
                } else {
                    yield Types.MapType.ofRequired(
                            idx.getAndIncrement(),
                            idx.getAndIncrement(),
                            Types.StringType.get(),
                            resolveAvroType(idx, valueType));
                }
            }
            case ARRAY -> {
                var elementType = schema.getElementType();
                var isOptional = elementType.isNullable();

                if (isOptional) {
                    yield Types.ListType.ofOptional(idx.getAndIncrement(), resolveAvroType(idx, elementType));
                } else {
                    yield Types.ListType.ofRequired(idx.getAndIncrement(), resolveAvroType(idx, elementType));
                }
            }
            case RECORD -> {
                var fields =
                        schema.getFields().stream().map(it -> resolveField(idx, it)).toList();
                yield Types.StructType.of(fields);
            }
            default -> throw new IllegalArgumentException("Unsupported avro schema type");
        };
    }

    private Type resolveAvroLogicalType(LogicalType logicalType, org.apache.avro.Schema avroValueSchema) {
        return switch (logicalType) {
            case LogicalTypes.Decimal type -> Types.DecimalType.of(type.getPrecision(), type.getScale());
            case LogicalTypes.Uuid ignored -> Types.UUIDType.get();
            case LogicalTypes.TimeMicros ignored -> Types.TimeType.get();
            case LogicalTypes.TimeMillis ignored -> Types.TimeType.get();
            case LogicalTypes.TimestampMicros ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield Types.TimestampType.withZone();
                } else {
                    yield Types.TimestampType.withoutZone();
                }
            }
            case LogicalTypes.TimestampMillis ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield Types.TimestampType.withZone();
                } else {
                    yield Types.TimestampType.withoutZone();
                }
            }
            case LogicalTypes.LocalTimestampMicros ignored -> Types.TimestampNanoType.withoutZone();
            case LogicalTypes.LocalTimestampMillis ignored -> Types.TimestampNanoType.withoutZone();
            case LogicalTypes.Date ignored -> Types.DateType.get();
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
    }
}
