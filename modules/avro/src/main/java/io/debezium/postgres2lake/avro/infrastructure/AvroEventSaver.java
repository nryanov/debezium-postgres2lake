package io.debezium.postgres2lake.avro.infrastructure;

import io.debezium.postgres2lake.avro.infrastructure.appender.AvroEventAppender;
import io.debezium.postgres2lake.avro.infrastructure.appender.AvroEventAppenderFactory;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.core.infrastructure.exceptions.InvalidOutputUriException;
import io.debezium.postgres2lake.core.infrastructure.exceptions.WriterOpenException;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.core.service.OutputLocationGenerator;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class AvroEventSaver extends AbstractEventSaver<AvroEventAppender> {
    private static final Logger logger = Logger.getLogger(AvroEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final CommonConfiguration.FileIO fileIO;
    private final AvroCompressionCodec codec;
    private final SchemaConverter<Schema> schemaConverter;
    private final AvroEventAppenderFactory appenderFactory;

    public AvroEventSaver(
            CommonConfiguration.Threshold threshold,
            OutputLocationGenerator outputLocationGenerator,
            CommonConfiguration.FileIO fileIO,
            AvroCompressionCodec codec,
            SchemaConverter<Schema> schemaConverter,
            AvroEventAppenderFactory appenderFactory,
            ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler
    ) {
        super(threshold, readinessMarkerEventEmitterHandler);
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
        this.codec = codec;

        this.schemaConverter = schemaConverter;
        this.appenderFactory = appenderFactory;
    }

    @Override
    protected AvroEventAppender createEventAppender(EventRecord event) {
        var location = outputLocationGenerator.generateLocation(event);

        try {
            logger.infof("Opening avro writer for `%s`", location);
            var path = new Path(new URI(location));

            var schema = schemaConverter.extractSchema(event);

            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var fs = FileSystem.get(new URI(location), config);
            var out = fs.create(path);
            var writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
                    .setCodec(codec.codec)
                    .create(schema, out);

            logger.infof("Successfully opened writer for `%s`", location);

            return appenderFactory.create(new AvroTableWriter(writer, schema, resolvePartition(event), location, event.destination()));
        } catch (URISyntaxException e) {
            logger.errorf("Invalid output URI: %s", location);
            throw new InvalidOutputUriException("Invalid output URI: " + location, e);
        } catch (IOException e) {
            logger.errorf(e, "Error happened while creating avro writer: %s", e.getLocalizedMessage());
            throw new WriterOpenException("Failed to open Avro writer for: " + location, e);
        }
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        return outputLocationGenerator.getPartition(event);
    }
}
