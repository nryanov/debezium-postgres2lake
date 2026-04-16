package io.debezium.postgres2lake.iceberg.infrastructure.s3;

import io.debezium.postgres2lake.iceberg.config.IcebergConfiguration;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.exceptions.IcebergTableAlterException;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.IcebergEventAppender;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.IcebergTableWriter;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.ddl.IcebergTableDdl;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.writer.IcebergWriterFactory;
import io.debezium.postgres2lake.core.infrastructure.schema.SchemaDiffResolver;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class S3IcebergEventSaver extends AbstractEventSaver<IcebergEventAppender> {
    private static final Logger logger = Logger.getLogger(S3IcebergEventSaver.class);

    private final Catalog catalog;
    private final IcebergWriterFactory writerFactory;
    private final IcebergTableDdl tableDdl;
    private final Map<String, IcebergConfiguration.IcebergTableSpec> tableSpecs;
    private final SchemaConverter<org.apache.iceberg.Schema> schemaConverter;
    private final SchemaDiffResolver schemaDiffResolver;

    public S3IcebergEventSaver(
            IcebergConfiguration configuration,
            SchemaConverter<Schema> schemaConverter,
            ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler
    ) {
        super(configuration.threshold(), readinessMarkerEventEmitterHandler);

        var catalogProperties = new HashMap<>(configuration.properties());

        var hadoopConfiguration = new Configuration();
        configuration.fileIO().properties().forEach(hadoopConfiguration::set);

        this.catalog = CatalogUtil.buildIcebergCatalog(configuration.name(), catalogProperties, hadoopConfiguration);
        this.writerFactory = new IcebergWriterFactory();
        this.tableDdl = new IcebergTableDdl(catalog);
        this.tableSpecs = new HashMap<>();
        this.tableSpecs.putAll(configuration.tableSpecs());
        this.schemaConverter = schemaConverter;
        this.schemaDiffResolver = new SchemaDiffResolver();
    }

    @Override
    protected IcebergEventAppender createEventAppender(EventRecord event) {
        var tableSchema = schemaConverter.extractSchema(event);
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var maybeTableSpec = Optional.ofNullable(tableSpecs.get(tableIdentifier.toString()));

        var table = tableDdl.createTableIfNotExists(tableIdentifier, tableSchema, maybeTableSpec);
        var tableWriter = new IcebergTableWriter(
                table,
                event.destination(),
                writerFactory.create(table),
                tableSchema,
                event.valueSchema()
        );

        return new IcebergEventAppender(tableWriter);
    }

    @Override
    protected void handleSchemaChanges(IcebergEventAppender appender, EventRecord event) {
        try {
            var diff = schemaDiffResolver.resolveDiff(appender.currentSchema(), event.valueSchema());
            tableDdl.handleSchemaEvolution(appender.table(), diff);
        } catch (Exception e) {
            throw new IcebergTableAlterException(e);
        }
    }
}
