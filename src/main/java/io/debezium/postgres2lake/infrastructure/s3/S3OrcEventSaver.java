package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcEventAppender;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcSchemaConverter;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3WriterOpenException;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcTableWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jboss.logging.Logger;

import java.io.IOException;

public class S3OrcEventSaver extends AbstractEventSaver<OrcEventAppender> {

    private static final Logger logger = Logger.getLogger(S3OrcEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;
    private final OrcCompressionCodec codec;
    private final SchemaConverter<TypeDescription> schemaConverter;

    public S3OrcEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputLocationGenerator outputLocationGenerator,
            OutputConfiguration.FileIO fileIO,
            OrcCompressionCodec codec
    ) {
        super(threshold);

        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
        this.codec = codec;
        this.schemaConverter = new CachedSchemaConverter<>(new OrcSchemaConverter());
    }

    @Override
    protected OrcEventAppender createEventAppender(EventRecord event) {
        var location = outputLocationGenerator.generateLocation("warehouse", event);
        var writer = createFileWriter(location, schemaConverter.extractSchema(event));
        var batch = writer.getSchema().createRowBatch(); // todo: configure batch size

        var tableWriter = new OrcTableWriter(writer, batch, event.valueSchema(), resolvePartition(event));
        return new OrcEventAppender(tableWriter);
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
        return outputLocationGenerator.getPartition("warehouse", event);
    }
}
