package io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.writer;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergWriterFactory {
    private final static DateTimeFormatter PARTITION_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    public BaseTaskWriter<Record> create(Table table) {
        var fileFormat = resolveTableFormat(table);
        var appender = createTableAppender(table);
        var fileFactory = createTableOutputFileFactory(table, fileFormat);

        var tableProperties = table.properties();

        var fileSize = PropertyUtil.propertyAsLong(
                tableProperties,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
        );

        var schema = table.schema();
        if (schema.identifierFieldIds().isEmpty()) {
            // no pk fields -> append only
            return appendOnlyWriter(table, fileFormat, appender, fileFactory, fileSize);
        } else {
            // use equality delete writer
            return equalityDeleteWriter(table, fileFormat, appender, fileFactory, fileSize);
        }
    }

    private BaseTaskWriter<Record> appendOnlyWriter(
            Table table,
            FileFormat format,
            GenericAppenderFactory appenderFactory,
            OutputFileFactory outputFileFactory,
            long fileSize
    ) {
        var partitionSpec = table.spec();

        if (partitionSpec.isPartitioned()) {
            return new PartitionedAppendOnlyWriter(
                    partitionSpec,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    fileSize,
                    table.schema()
            );
        } else {
            return new UnpartitionedAppendOnlyWriter(
                    partitionSpec,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    fileSize
            );
        }
    }

    private BaseTaskWriter<Record> equalityDeleteWriter(
            Table table,
            FileFormat format,
            GenericAppenderFactory appenderFactory,
            OutputFileFactory outputFileFactory,
            long fileSize
    ) {
        var partitionSpec = table.spec();

        if (partitionSpec.isPartitioned()) {
            return new PartitionedEqualityDeleteWriter(
                    partitionSpec,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    fileSize,
                    table.schema()
            );
        } else {
            return new UnpartitionedEqualityDeleteWriter(
                    partitionSpec,
                    format,
                    appenderFactory,
                    outputFileFactory,
                    table.io(),
                    fileSize,
                    table.schema()
            );
        }
    }

    private FileFormat resolveTableFormat(Table table) {
        String formatAsString = table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
    }

    private GenericAppenderFactory createTableAppender(Table table) {
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
        var partitionId = Integer.parseInt(PARTITION_ID_FORMATTER.format(Instant.now()));
        var taskId = 1L;

        return OutputFileFactory.builderFor(table, partitionId, taskId)
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
    }
}
