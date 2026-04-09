package io.debezium.postgres2lake.extensions.data.catalog.datahub;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.ArrayType;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.BytesType;
import com.linkedin.schema.DateType;
import com.linkedin.schema.EnumType;
import com.linkedin.schema.FixedType;
import com.linkedin.schema.MapType;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.RecordType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schema.TimeType;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableColumnType;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableField;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogPropertyReader.required;
import static io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogPropertyReader.optional;

/**
 * Publishes dataset properties and schema to DataHub using the REST emitter (datahub-client 1.x).
 *
 * <p>Required properties:
 * <ul>
 *   <li>{@code datahub.server} — GMS base URL (e.g. {@code http://localhost:8080})</li>
 * </ul>
 * Optional:
 * <ul>
 *   <li>{@code datahub.token} — bearer token for authenticated GMS</li>
 *   <li>{@code datahub.fabric} — {@link FabricType} name (default {@code PROD})</li>
 * </ul>
 */
public final class DataHubDataCatalogHandler implements DataCatalogHandler {

    private volatile RestEmitter emitter;
    private volatile DataPlatformUrn platform;
    private volatile FabricType fabric;

    public DataHubDataCatalogHandler() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");

        var server = required(properties, "datahub.server");
        var token = optional(properties, "datahub.token", "");
        var fabricKey = optional(properties, "datahub.fabric", "PROD").toUpperCase(Locale.ROOT);

        this.platform = new DataPlatformUrn("postgres");
        this.fabric = FabricType.valueOf(fabricKey);

        this.emitter = RestEmitter.create(builder -> {
            builder.server(server);
            if (!token.isEmpty()) {
                builder.token(token);
            }
        });
    }

    @Override
    public void createOrUpdateTable(TableDestination destination, TableSchema schema) {
        var datasetName = destination.database() + "." + destination.schema() + "." + destination.table();
        var urn = new DatasetUrn(platform, datasetName, fabric);

        var props = new DatasetProperties();
        props.setName(destination.table());
        props.setQualifiedName(datasetName);

        emit(mcpForDataset(urn, props));

        List<SchemaField> fields = new ArrayList<>();
        for (var field : schema.fields()) {
            fields.add(mapToSchemaField(field));
        }

        var meta = new SchemaMetadata();
        meta.setFields(new SchemaFieldArray(fields));
        meta.setPlatform(platform);
        meta.setSchemaName(datasetName);
        meta.setVersion(0L);
        meta.setHash("");
        meta.setPlatformSchema(SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("")));

        emit(mcpForDataset(urn, meta));
    }

    private static MetadataChangeProposalWrapper<?> mcpForDataset(DatasetUrn urn, RecordTemplate aspect) {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn(urn)
                .upsert()
                .aspect(aspect)
                .build();
    }

    private SchemaField mapToSchemaField(TableField field) {
        var schemaField = new SchemaField();
        schemaField.setFieldPath(field.name());
        schemaField.setNullable(isNullable(field.type().constraint()));
        schemaField.setNativeDataType(field.type().getClass().getName());
        schemaField.setType(mapToSchemaFieldType(field.type()));
        field.description().ifPresent(schemaField::setDescription);

        return schemaField;
    }

    private boolean isNullable(TableColumnType.TableColumnConstraint constraint) {
        return switch (constraint) {
            case OPTIONAL -> true;
            default -> false;
        };
    }

    private SchemaFieldDataType mapToSchemaFieldType(TableColumnType type) {
        return switch (type) {
            case TableColumnType.PrimitiveColumnType p -> switch (p) {
                    case TableColumnType.Boolean ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType()));
                    case TableColumnType.Bytes ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()));
                    case TableColumnType.Date ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new DateType()));
                    case TableColumnType.Decimal ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
                    case TableColumnType.Double ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
                    case TableColumnType.Enum ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new EnumType()));
                    case TableColumnType.Fixed ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new FixedType()));
                    case TableColumnType.Float ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
                    case TableColumnType.Int ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
                    case TableColumnType.Long ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
                    case TableColumnType.Text ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
                    case TableColumnType.Time ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
                    case TableColumnType.Timestamp ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
                    case TableColumnType.TimestampTz ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new TimeType()));
                    case TableColumnType.Uuid ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
            };
            case TableColumnType.ComplexColumnType c -> switch (c) {
                case TableColumnType.Array ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new ArrayType()));
                case TableColumnType.Map ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new MapType()));
                case TableColumnType.Record ignored -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new RecordType()));
            };
        };
    }

    // TODO: move to another thread
    private void emit(MetadataChangeProposalWrapper<?> mcp) {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        try {
            emitter.emit(
                    mcp,
                    new Callback() {
                        @Override
                        public void onCompletion(MetadataWriteResponse metadataWriteResponse) {
                            done.countDown();
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            failure.set(throwable);
                            done.countDown();
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("DataHub emit failed", e);
        }
        try {
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new RuntimeException("DataHub emit timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("DataHub emit interrupted", e);
        }
        Throwable t = failure.get();
        if (t != null) {
            throw new RuntimeException("DataHub rejected metadata change", t);
        }
    }
}
