package io.debezium.postgres2lake.extensions.data.catalog.datahub;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.schema.BooleanType;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.NumberType;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableColumnType;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
 *   <li>{@code datahub.platform} — data platform name (default {@code postgres})</li>
 *   <li>{@code datahub.fabric} — {@link FabricType} name (default {@code PROD})</li>
 * </ul>
 */
public final class DataHubDataCatalogHandler implements DataCatalogHandler {

    private volatile RestEmitter emitter;
    private volatile String platformName;
    private volatile FabricType fabric;

    public DataHubDataCatalogHandler() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Objects.requireNonNull(properties, "properties");

        var server = required(properties, "datahub.server");
        var token = properties.getOrDefault("datahub.token", "").trim();
        var fabricKey = properties.getOrDefault("datahub.fabric", "PROD").trim().toUpperCase(Locale.ROOT);

        this.platformName = properties.getOrDefault("datahub.platform", "postgres").trim();

        if (this.platformName.isEmpty()) {
            throw new IllegalArgumentException("datahub.platform must be non-blank when set");
        }

        this.fabric = FabricType.valueOf(fabricKey);

        this.emitter = RestEmitter.create(b -> {
            b.server(server);
            if (!token.isEmpty()) {
                b.token(token);
            }
        });
    }

    @Override
    public void createOrUpdateTable(TableDestination destination, TableSchema schema) {
        var datasetName = destination.database() + "." + destination.schema() + "." + destination.table();
        var urn = new DatasetUrn(new DataPlatformUrn(platformName), datasetName, fabric);

        DatasetProperties props = new DatasetProperties();
        props.setName(destination.table());
        props.setQualifiedName(datasetName);

        emit(emitter, mcpForDataset(urn, props));

        List<SchemaField> fields = new ArrayList<>();
        for (var field : schema.fields()) {
            fields.add(toSchemaField(field));
        }

        var meta = new SchemaMetadata();
        meta.setFields(new SchemaFieldArray(fields));
        meta.setPlatform(new DataPlatformUrn(platformName));
        meta.setSchemaName(datasetName);
        meta.setVersion(0L);
        meta.setHash("");
        meta.setPlatformSchema(
                SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema("")));

        emit(emitter, mcpForDataset(urn, meta));
    }

    private static MetadataChangeProposalWrapper mcpForDataset(DatasetUrn urn, com.linkedin.data.template.RecordTemplate aspect) {
        return MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn(urn)
                .upsert()
                .aspect(aspect)
                .build();
    }

    private static void emit(RestEmitter emitter, MetadataChangeProposalWrapper mcp) {
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

    private static SchemaField toSchemaField(TableColumnType col) {
        SchemaField sf = new SchemaField();
        sf.setFieldPath(col.name());
        sf.setType(mapDataHubType(col.type()));
        sf.setNativeDataType(col.type());
        sf.setNullable(col.nullable());
        col.description().ifPresent(sf::setDescription);
        return sf;
    }

    private static SchemaFieldDataType mapDataHubType(String raw) {
        String t = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        return switch (t) {
            case "boolean", "bool" -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BooleanType()));
            case "int", "integer", "long", "bigint", "short", "byte", "float", "double", "decimal", "number" ->
                    new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
            default -> new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
        };
    }

    private static String required(Map<String, String> properties, String key) {
        String v = properties.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return v.trim();
    }
}
