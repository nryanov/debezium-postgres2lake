package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.iceberg.AvroToIcebergMapper;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergTableWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class S3IcebergEventSaver extends AbstractEventSaver<IcebergTableWriter> {
    private static final Logger logger = Logger.getLogger(S3IcebergEventSaver.class);

    private final Catalog catalog;
    private final AvroToIcebergMapper mapper;

    public S3IcebergEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputConfiguration.Iceberg icebergCfg
    ) {
        super(threshold);

        var catalogProperties = new HashMap<>(icebergCfg.properties());

        var hadoopConfiguration = new Configuration();
        icebergCfg.fileIO().ifPresent(cfg -> cfg.properties().forEach(hadoopConfiguration::set));

        this.catalog = CatalogUtil.buildIcebergCatalog(icebergCfg.name(), catalogProperties, hadoopConfiguration);
        this.mapper = new AvroToIcebergMapper();
    }

    @Override
    protected IcebergTableWriter createWriter(EventRecord event) {
        try {
            // todo: extract into separate logic
            var namespaceCatalog = (SupportsNamespaces) catalog;
            var ns = Namespace.of("development");

            namespaceCatalog.createNamespace(ns);
            var tableIdentifier = TableIdentifier.of(ns, "data");
            var tableSchema = mapper.avroToIcebergSchema(event.key().getSchema(), event.value().getSchema());
            catalog.createTable(tableIdentifier, tableSchema);
        } catch (Exception e) {
            logger.errorf(e, "Error happened while creating namespace/table: %s", e.getLocalizedMessage());
        }

        var tableIdentifier = TableIdentifier.of(Namespace.of("development"), "data");
        var table = catalog.loadTable(tableIdentifier);

        // todo: currently it is append-only writer
        return new IcebergTableWriter(table, createTableWriter(table));
    }

    @Override
    protected void appendEvent(EventRecord event, IcebergTableWriter wrapper) throws IOException {
        var icebergSchema = mapper.avroToIcebergSchema(event.key().getSchema(), event.value().getSchema());
        var record = mapper.createIcebergRecord(icebergSchema, event.value());
        wrapper.writer().write(record);
    }

    @Override
    protected void commitPendingEvents(IcebergTableWriter wrapper) throws IOException {
        var rs = wrapper.writer().complete();
        // now only append-only logic is supported => ignore delete files
        var dataFiles = rs.dataFiles();
        var appendIo = wrapper.table().newAppend();
        Arrays.stream(dataFiles).forEach(appendIo::appendFile);
        appendIo.commit();
    }

    private BaseTaskWriter<Record> createTableWriter(Table table) {
        var fileFormat = resolveTableFormat(table);
        var appenderFactory = createTableAppender(table);
        var outputFileFactory = createTableOutputFileFactory(table, fileFormat);

        var fileSize = 10 * 1024 * 1024; // todo: get from config

        // todo: resolve partitioned or unpartitioned writer
        return new UnpartitionedWriter<>(
                table.spec(), fileFormat, appenderFactory, outputFileFactory, table.io(), fileSize);
    }

    private FileFormat resolveTableFormat(Table table) {
        String formatAsString = table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
    }

    public static GenericAppenderFactory createTableAppender(Table table) {
        final Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
        if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
            return new GenericAppenderFactory(
                    table.schema(),
                    table.spec(),
                    null,
                    null,
                    null)
                    .setAll(table.properties());
        } else {
            return new GenericAppenderFactory(
                    table.schema(),
                    table.spec(),
                    identifierFieldIds.stream().mapToInt(it -> it).toArray(),
                    TypeUtil.select(table.schema(), new HashSet<>(identifierFieldIds)),
                    null)
                    .setAll(table.properties());
        }
    }

    private OutputFileFactory createTableOutputFileFactory(Table table, FileFormat format) {
        var partitionId = 1; // todo: generate partition id
        var taskId = 1L; // todo: generate task id

        return OutputFileFactory.builderFor(table, partitionId, taskId)
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
    }
}
