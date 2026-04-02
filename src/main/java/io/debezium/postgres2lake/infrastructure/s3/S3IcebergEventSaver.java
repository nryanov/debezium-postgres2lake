package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergEventAppender;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergSchemaConverter;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergTableWriter;
import io.debezium.postgres2lake.infrastructure.format.iceberg.ddl.IcebergTableDdl;
import io.debezium.postgres2lake.infrastructure.format.iceberg.writer.IcebergWriterFactory;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class S3IcebergEventSaver extends AbstractEventSaver<IcebergTableWriter> {
    private static final Logger logger = Logger.getLogger(S3IcebergEventSaver.class);

    private final Catalog catalog;
    private final IcebergWriterFactory writerFactory;
    private final IcebergTableDdl tableDdl;
    private final Map<String, OutputConfiguration.IcebergTableSpec> tableSpecs;
    private final EventAppender<IcebergTableWriter> eventAppender;
    private final SchemaConverter<org.apache.iceberg.Schema> schemaConverter;

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
        this.eventAppender = new IcebergEventAppender();
        this.schemaConverter = new IcebergSchemaConverter();
    }

    @Override
    protected IcebergTableWriter createWriter(EventRecord event) {
        var tableSchema = schemaConverter.extractSchema(event);
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var maybeTableSpec = Optional.ofNullable(tableSpecs.get(tableIdentifier.name()));

        var table = tableDdl.createTableIfNotExists(tableIdentifier, tableSchema, maybeTableSpec);

        return new IcebergTableWriter(table, writerFactory.create(table), tableSchema, event.valueSchema());
    }

    @Override
    protected void handleSchemaChanges(EventRecord event, Schema currentSchema) {
        // TODO: execute TABLE ALTER
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        // iceberg resolve partition in tableIo
        return "";
    }

    @Override
    protected void appendEvent(EventRecord event, IcebergTableWriter wrapper) throws Exception {
        eventAppender.appendEvent(event, wrapper);
    }

    @Override
    protected void commitPendingEvents(IcebergTableWriter wrapper) throws IOException {
        var rs = wrapper.writer().complete();

        if (rs.deleteFiles().length > 0) {
            var delta = wrapper.table().newRowDelta();
            var dataFiles = rs.dataFiles();
            var deleteFiles = rs.deleteFiles();
            Arrays.stream(dataFiles).forEach(delta::addRows);
            Arrays.stream(deleteFiles).forEach(delta::addDeletes);
            delta.commit();
        } else {
            var dataFiles = rs.dataFiles();
            var appendIo = wrapper.table().newAppend();
            Arrays.stream(dataFiles).forEach(appendIo::appendFile);
            appendIo.commit();
        }
    }
}
