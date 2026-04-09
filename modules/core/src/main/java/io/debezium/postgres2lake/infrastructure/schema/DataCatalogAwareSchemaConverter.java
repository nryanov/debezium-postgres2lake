package io.debezium.postgres2lake.infrastructure.schema;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableColumnType;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableField;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class DataCatalogAwareSchemaConverter<T> implements SchemaConverter<T> {
    private final static Logger logger = Logger.getLogger(DataCatalogAwareSchemaConverter.class);

    private final SchemaConverter<T> delegate;
    private final DataCatalogHandler dataCatalogHandler;

    public DataCatalogAwareSchemaConverter(SchemaConverter<T> delegate, DataCatalogHandler dataCatalogHandler) {
        this.delegate = delegate;
        this.dataCatalogHandler = dataCatalogHandler;
    }

    @Override
    public T extractSchema(EventRecord event) {
        try {
            if (isNewSchema(event)) {
                var destination = createTableDestination(event);
                var schema = createTableSchema(event);

                dataCatalogHandler.createOrUpdateTable(destination, schema);
            }
        } catch (Exception e) {
            logger.warnf(e, "Error happened during createOrUpdateTable for destination %s", event.rawDestination());
        }

        return delegate.extractSchema(event);
    }

    @Override
    public boolean isNewSchema(EventRecord event) {
        return delegate.isNewSchema(event);
    }

    private TableDestination createTableDestination(EventRecord event) {
        var destination = event.destination();
        return new TableDestination(destination.database(), destination.schema(), destination.table());
    }

    private TableSchema createTableSchema(EventRecord event) {
        var keys = event.keySchema().getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
        var fields = new ArrayList<TableField>();

        var valueSchema = event.valueSchema();

        for (var field : valueSchema.getFields()) {
            fields.add(avroToTableField(keys, field));
        }

        return new TableSchema(fields);
    }

    private TableField avroToTableField(Set<String> keys, Schema.Field field) {
        var constraint = mapToConstraint(keys, field);
        return new TableField(field.name(), field.doc(), avroToTableColumnType(field.schema(), constraint));
    }

    private TableColumnType avroToTableColumnType(Schema schema, TableColumnType.TableColumnConstraint constraint) {
        var logicalType = schema.getLogicalType();
        if (logicalType != null) {
            return avroLogicalToTableColumnType(constraint, logicalType, schema);
        }

        return switch (schema.getType()) {
            case INT -> new TableColumnType.Int(constraint);
            case STRING -> new TableColumnType.Text(constraint);
            case ENUM -> new TableColumnType.Enum(constraint);
            case BOOLEAN -> new TableColumnType.Boolean(constraint);
            case LONG -> new TableColumnType.Long(constraint);
            case FLOAT -> new TableColumnType.Float(constraint);
            case DOUBLE -> new TableColumnType.Double(constraint);
            case FIXED -> new TableColumnType.Fixed(constraint);
            case BYTES -> new TableColumnType.Bytes(constraint);
            case UNION -> {
                // use first not null paimonSchema
                if (schema.getType() == Schema.Type.UNION) {
                    for (var s : schema.getTypes()) {
                        if (s.getType() != Schema.Type.NULL) yield avroToTableColumnType(s, constraint);
                    }
                }

                throw new IllegalArgumentException("Unsupported type");
            }
            case MAP -> new TableColumnType.Map(
                    constraint,
                    new TableColumnType.Text(TableColumnType.TableColumnConstraint.REQUIRED),
                    avroToTableColumnType(schema.getValueType(), mapToConstraint(schema.getValueType()))
            );
            case ARRAY -> new TableColumnType.Array(
                    constraint,
                    avroToTableColumnType(schema.getElementType(), mapToConstraint(schema.getElementType()))
            );
            case RECORD -> {
                var nestedFields = new ArrayList<TableField>();

                for (var nestedField : schema.getFields()) {
                    nestedFields.add(avroToTableField(Collections.emptySet(), nestedField));
                }

                yield new TableColumnType.Record(constraint, nestedFields);
            }
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    private TableColumnType avroLogicalToTableColumnType(TableColumnType.TableColumnConstraint constraint, LogicalType logicalType, Schema avroValueSchema) {
        return switch (logicalType) {
            case LogicalTypes.Decimal decimal -> new TableColumnType.Decimal(constraint, decimal.getScale(), decimal.getPrecision());
            case LogicalTypes.Uuid ignored ->  new TableColumnType.Uuid(constraint);
            case LogicalTypes.TimeMicros ignored -> new TableColumnType.Time(constraint);
            case LogicalTypes.TimeMillis ignored -> new TableColumnType.Time(constraint);
            case LogicalTypes.TimestampMicros ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield new TableColumnType.TimestampTz(constraint);
                } else {
                    yield new TableColumnType.Timestamp(constraint);
                }
            }
            case LogicalTypes.TimestampMillis ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield new TableColumnType.TimestampTz(constraint);
                } else {
                    yield new TableColumnType.Timestamp(constraint);
                }
            }
            case LogicalTypes.LocalTimestampMicros ignored -> new TableColumnType.Timestamp(constraint);
            case LogicalTypes.LocalTimestampMillis ignored -> new TableColumnType.Timestamp(constraint);
            case LogicalTypes.Date ignored -> new TableColumnType.Date(constraint);
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
    }

    private TableColumnType.TableColumnConstraint mapToConstraint(Set<String> keys, Schema.Field field) {
        if (keys.contains(field.name())) {
            return TableColumnType.TableColumnConstraint.PRIMARY_KEY;
        }

        if (field.hasDefaultValue() && field.defaultVal() != JsonProperties.NULL_VALUE) {
            return TableColumnType.TableColumnConstraint.REQUIRED;
        } else if (field.defaultVal() == JsonProperties.NULL_VALUE) {
            return TableColumnType.TableColumnConstraint.OPTIONAL;
        } else {
            return TableColumnType.TableColumnConstraint.REQUIRED;
        }
    }

    private TableColumnType.TableColumnConstraint mapToConstraint(Schema schema) {
        if (schema.isNullable()) {
            return TableColumnType.TableColumnConstraint.OPTIONAL;
        }

        return TableColumnType.TableColumnConstraint.REQUIRED;
    }
}
