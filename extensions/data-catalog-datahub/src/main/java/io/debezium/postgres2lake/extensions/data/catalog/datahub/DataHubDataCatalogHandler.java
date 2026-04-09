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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
 *   <li>{@code datahub.platform} — platform name (default {@code postgres})</li>
 * </ul>
 */
public final class DataHubDataCatalogHandler implements DataCatalogHandler {
    private final static Logger logger = LoggerFactory.getLogger(DataHubDataCatalogHandler.class);

    private RestEmitter emitter;
    private DataPlatformUrn platform;
    private FabricType fabric;

    public DataHubDataCatalogHandler() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");

        var server = required(properties, "datahub.server");
        var token = optional(properties, "datahub.token", "");
        var platformName = optional(properties, "datahub.platform", "postgres");
        var fabricKey = optional(properties, "datahub.fabric", "PROD").toUpperCase(Locale.ROOT);

        this.platform = new DataPlatformUrn(platformName);
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

        emit(dataSetChangeProposal(urn, props));

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

        emit(dataSetChangeProposal(urn, meta));
    }

    @Override
    public void close() {
        try {
            emitter.close();
        } catch (IOException e) {
            logger.error("Error happened while closing emitter", e);
        }
    }

    private MetadataChangeProposalWrapper<?> dataSetChangeProposal(DatasetUrn urn, RecordTemplate aspect) {
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

    private void emit(MetadataChangeProposalWrapper<?> mcp) {
        try {
            emitter.emit(
                    mcp,
                    new Callback() {
                        @Override
                        public void onCompletion(MetadataWriteResponse metadataWriteResponse) {}

                        @Override
                        public void onFailure(Throwable e) {
                            logger.error("Error happened while emitting proposal change", e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Error happened while emitting proposal change", e);
        }
    }
}
