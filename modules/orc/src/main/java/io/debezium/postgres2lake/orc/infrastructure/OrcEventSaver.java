package io.debezium.postgres2lake.orc.infrastructure;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.core.infrastructure.s3.exceptions.S3WriterOpenException;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.core.service.OutputLocationGenerator;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.orc.infrastructure.appender.OrcEventAppender;
import io.debezium.postgres2lake.orc.infrastructure.appender.OrcEventAppenderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jboss.logging.Logger;

import java.io.IOException;

public class OrcEventSaver extends AbstractEventSaver<OrcEventAppender> {

    private static final Logger logger = Logger.getLogger(OrcEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final CommonConfiguration.FileIO fileIO;
    private final OrcCompressionCodec codec;
    private final SchemaConverter<TypeDescription> schemaConverter;
    private final OrcEventAppenderFactory appenderFactory;
    private final int rowBatchSize;

    public OrcEventSaver(
            CommonConfiguration.Threshold threshold,
            OutputLocationGenerator outputLocationGenerator,
            CommonConfiguration.FileIO fileIO,
            OrcCompressionCodec codec,
            SchemaConverter<TypeDescription> schemaConverter,
            OrcEventAppenderFactory appenderFactory,
            ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler,
            int rowBatchSize
    ) {
        super(threshold, readinessMarkerEventEmitterHandler);

        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
        this.codec = codec;
        this.schemaConverter = schemaConverter;
        this.appenderFactory = appenderFactory;
        this.rowBatchSize = rowBatchSize;
    }

    @Override
    protected OrcEventAppender createEventAppender(EventRecord event) {
        var location = outputLocationGenerator.generateLocation(event);
        var writer = createFileWriter(location, schemaConverter.extractSchema(event));
        var batch = writer.getSchema().createRowBatch(rowBatchSize);

        var tableWriter = new OrcTableWriter(writer, batch, event.valueSchema(), resolvePartition(event), location, event.destination());
        return appenderFactory.create(tableWriter);
    }

    private Writer createFileWriter(String location, TypeDescription schema) {
        try {
            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var options = OrcFile.writerOptions(config)
                    .setSchema(schema)
                    .stripeSize(64 * 1024 * 1024) // 64 Mb
                    .useUTCTimestamp(true)
                    .compress(codec.codec);


            return OrcFile.createWriter(new Path(location), options);
        } catch (IOException e) {
            logger.errorf(e, "Error happened while creating ORC writer: %s", e.getLocalizedMessage());
            throw new S3WriterOpenException("Failed to open ORC writer for: " + location, e);
        }
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        return outputLocationGenerator.getPartition(event);
    }
}
