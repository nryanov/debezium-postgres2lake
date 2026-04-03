package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.iceberg.exceptions.IcebergTableAlterException;
import io.debezium.postgres2lake.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergEventAppender;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergSchemaConverter;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergTableWriter;
import io.debezium.postgres2lake.infrastructure.format.iceberg.ddl.IcebergTableDdl;
import io.debezium.postgres2lake.infrastructure.format.iceberg.writer.IcebergWriterFactory;
import io.debezium.postgres2lake.infrastructure.schema.SchemaDiffResolver;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
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
    private final Map<String, OutputConfiguration.IcebergTableSpec> tableSpecs;
    private final SchemaConverter<org.apache.iceberg.Schema> schemaConverter;
    private final SchemaDiffResolver schemaDiffResolver;

    public S3IcebergEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputConfiguration.Iceberg icebergCfg
    ) {
        super(threshold);

        var catalogProperties = new HashMap<>(icebergCfg.properties());

        var hadoopConfiguration = new Configuration();
        icebergCfg.fileIO().ifPresent(cfg -> cfg.properties().forEach(hadoopConfiguration::set));

        this.catalog = CatalogUtil.buildIcebergCatalog(icebergCfg.name(), catalogProperties, hadoopConfiguration);
        this.writerFactory = new IcebergWriterFactory();
        this.tableDdl = new IcebergTableDdl(catalog);
        this.tableSpecs = new HashMap<>();
        this.tableSpecs.putAll(icebergCfg.tableSpecs());
        this.schemaConverter = new CachedSchemaConverter<>(new IcebergSchemaConverter());
        this.schemaDiffResolver = new SchemaDiffResolver();
    }

    @Override
    protected IcebergEventAppender createEventAppender(EventRecord event) {
        var tableSchema = schemaConverter.extractSchema(event);
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var maybeTableSpec = Optional.ofNullable(tableSpecs.get(tableIdentifier.name()));

        var table = tableDdl.createTableIfNotExists(tableIdentifier, tableSchema, maybeTableSpec);
        var tableWriter = new IcebergTableWriter(table, writerFactory.create(table), tableSchema, event.valueSchema());

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
