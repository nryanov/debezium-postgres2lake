package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroEventAppender;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroSchemaConverter;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroTableWriter;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3InvalidOutputUriException;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3WriterOpenException;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.config.CommonConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
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

public class S3AvroEventSaver extends AbstractEventSaver<AvroEventAppender> {
    private static final Logger logger = Logger.getLogger(S3AvroEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final CommonConfiguration.FileIO fileIO;
    private final AvroCompressionCodec codec;

    private final SchemaConverter<Schema> schemaConverter;

    public S3AvroEventSaver(
            CommonConfiguration.Threshold threshold,
            OutputLocationGenerator outputLocationGenerator,
            CommonConfiguration.FileIO fileIO,
            AvroCompressionCodec codec
    ) {
        super(threshold);
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
        this.codec = codec;

        this.schemaConverter = new AvroSchemaConverter();
    }

    @Override
    protected AvroEventAppender createEventAppender(EventRecord event) {
        var location = outputLocationGenerator.generateLocation("warehouse", event);

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

            return new AvroEventAppender(new AvroTableWriter(writer, schema, resolvePartition(event)));
        } catch (URISyntaxException e) {
            logger.errorf("Invalid output URI: %s", location);
            throw new S3InvalidOutputUriException("Invalid output URI: " + location, e);
        } catch (IOException e) {
            logger.errorf(e, "Error happened while creating avro writer: %s", e.getLocalizedMessage());
            throw new S3WriterOpenException("Failed to open Avro writer for: " + location, e);
        }
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        return outputLocationGenerator.getPartition("warehouse", event);
    }
}
