package io.debezium.postgres2lake.extensions.data.catalog.openmetadata;

import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableColumnType;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableField;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.client.model.Column;
import org.openmetadata.client.model.CreateTable;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Creates or updates a table in OpenMetadata via {@link TablesApi#createOrUpdateTable}.
 *
 * <p>Required properties:
 * <ul>
 *   <li>{@code openmetadata.host} — server URL including {@code /api} (e.g. {@code http://localhost:8585/api})</li>
 *   <li>{@code openmetadata.jwt} — JWT for bot/service account ({@link AuthProvider#OPENMETADATA})</li>
 *   <li>{@code openmetadata.databaseSchema.fqn} — fully qualified name of the parent database schema entity</li>
 * </ul>
 * Optional:
 * <ul>
 *   <li>{@code openmetadata.validate.version} — when {@code true}, validates client vs server catalog version on connect (default {@code false})</li>
 * </ul>
 */
public final class OpenMetadataDataCatalogHandler implements DataCatalogHandler {

    private volatile OpenMetadata gateway;
    private volatile TablesApi tablesApi;
    private volatile String databaseSchemaFqn;

    public OpenMetadataDataCatalogHandler() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");
        String host = required(properties, "openmetadata.host");
        String jwt = required(properties, "openmetadata.jwt");
        this.databaseSchemaFqn = required(properties, "openmetadata.databaseSchema.fqn");

        boolean validateVersion = Boolean.parseBoolean(properties.getOrDefault("openmetadata.validate.version", "false"));

        OpenMetadataConnection conn = new OpenMetadataConnection();
        conn.setHostPort(host);
        conn.setApiVersion("v1");
        conn.setAuthProvider(AuthProvider.OPENMETADATA);
        conn.setSecurityConfig(new OpenMetadataJWTClientConfig().withJwtToken(jwt));

        this.gateway = new OpenMetadata(conn, validateVersion);
        this.tablesApi = this.gateway.buildClient(TablesApi.class);
    }

    @Override
    public void createOrUpdateTable(TableDestination destination, TableSchema schema) {
        TablesApi api = this.tablesApi;
        String schemaFqn = this.databaseSchemaFqn;
        if (api == null || schemaFqn == null) {
            throw new IllegalStateException("DataCatalogHandler not initialized; call initialize() first");
        }

        var columns = new ArrayList<Column>();
        for (var field : schema.fields()) {
            columns.add(mapToColumn(field));
        }

        var body = new CreateTable()
                .name(destination.table())
                .databaseSchema(schemaFqn)
                .columns(columns)
                .tableType(CreateTable.TableTypeEnum.REGULAR);

        api.createOrUpdateTable(body);
    }

    private Column mapToColumn(TableField field) {
        var column = new Column();
        column.name(field.name());
        column.constraint(mapToConstraint(field.type().constraint()));
        field.description().ifPresent(column::description);

        mapToDataType(column, field.type());

        return column;
    }

    private void mapToDataType(Column column, TableColumnType type) {
        switch (type) {
            case TableColumnType.PrimitiveColumnType p -> {
                switch (p) {
                    case TableColumnType.Boolean v -> column.dataType(Column.DataTypeEnum.BOOLEAN);
                    case TableColumnType.Bytes v -> column.dataType(Column.DataTypeEnum.BYTES);
                    case TableColumnType.Date v -> column.dataType(Column.DataTypeEnum.DATE);
                    case TableColumnType.Decimal v -> {
                        column.dataType(Column.DataTypeEnum.DECIMAL);
                        column.setScale(v.scale());
                        column.setPrecision(v.precision());
                    }
                    case TableColumnType.Double v -> column.dataType(Column.DataTypeEnum.DOUBLE);
                    case TableColumnType.Enum v -> column.dataType(Column.DataTypeEnum.ENUM);
                    case TableColumnType.Fixed v -> column.dataType(Column.DataTypeEnum.FIXED);
                    case TableColumnType.Float v -> column.dataType(Column.DataTypeEnum.FLOAT);
                    case TableColumnType.Int v -> column.dataType(Column.DataTypeEnum.INT);
                    case TableColumnType.Long v -> column.dataType(Column.DataTypeEnum.LONG);
                    case TableColumnType.Text v -> column.dataType(Column.DataTypeEnum.TEXT);
                    case TableColumnType.Time v -> column.dataType(Column.DataTypeEnum.TIME);
                    case TableColumnType.Timestamp v -> column.dataType(Column.DataTypeEnum.TIMESTAMP);
                    case TableColumnType.TimestampTz v -> column.dataType(Column.DataTypeEnum.TIMESTAMPZ);
                    case TableColumnType.Uuid v -> column.dataType(Column.DataTypeEnum.UUID);
                }
            }
            case TableColumnType.ComplexColumnType c -> {
                switch (c) {
                    case TableColumnType.Array array -> {
                        column.setDataType(Column.DataTypeEnum.ARRAY);
                        var elementColumn = new Column();
                        mapToDataType(elementColumn, array.element());
                        var arrayElementType = Column.ArrayDataTypeEnum.fromValue(elementColumn.getDataType().getValue());
                        column.arrayDataType(arrayElementType);
                    }
                    case TableColumnType.Map map -> {
                        column.dataType(Column.DataTypeEnum.MAP);

                        var key = new Column();
                        key.name("key");
                        key.ordinalPosition(1);
                        key.constraint(Column.ConstraintEnum.NOT_NULL); // always not null
                        mapToDataType(key, map.key());

                        var value = new Column();
                        value.name("value");
                        value.ordinalPosition(2);
                        value.constraint(mapToConstraint(map.value().constraint()));
                        mapToDataType(value, map.value());

                        column.children(List.of(key, value));
                    }
                    case TableColumnType.Record record -> {
                        column.dataType(Column.DataTypeEnum.RECORD);

                        var nestedColumns = new ArrayList<Column>();
                        var ordinalPosition = 1;

                        for (var nestedField : record.nestedFields()) {
                            var nestedColumn = new Column();
                            nestedColumn.name(nestedField.name());
                            nestedColumn.constraint(mapToConstraint(nestedField.type().constraint()));
                            nestedField.description().ifPresent(column::description);
                            nestedColumn.ordinalPosition(ordinalPosition);

                            mapToDataType(nestedColumn, nestedField.type());

                            nestedColumns.add(nestedColumn);
                            ordinalPosition++;
                        }

                        column.children(nestedColumns);
                    }
                }
            }
        }
    }

    private Column.ConstraintEnum mapToConstraint(TableColumnType.TableColumnConstraint constraint) {
        return switch (constraint) {
            case OPTIONAL -> Column.ConstraintEnum.NULL;
            case REQUIRED -> Column.ConstraintEnum.NOT_NULL;
            case PRIMARY_KEY -> Column.ConstraintEnum.PRIMARY_KEY;
        };
    }

    private static String required(Map<String, String> properties, String key) {
        var v = properties.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return v.trim();
    }
}
