package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
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

public class S3AvroEventSaver extends AbstractEventSaver<DataFileWriter<GenericRecord>> {
    private static final Logger logger = Logger.getLogger(S3AvroEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;

    public S3AvroEventSaver(OutputLocationGenerator outputLocationGenerator, OutputConfiguration.FileIO fileIO) {
        super();
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
    }

    @Override
    protected DataFileWriter<GenericRecord> createWriter(EventRecord event) {
        try {
            var location = outputLocationGenerator.generateLocation("warehouse", event);
            logger.infof("Opening parquet writer for `%s`", location);
            var path = new Path(new URI(location));

            var schema = event.value().getSchema();

            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var fs = FileSystem.get(new URI(location), config);
            var out = fs.create(path);
            // todo: allow to setup codec
            var writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, out);

            logger.infof("Successfully opened writer for `%s`", location);

            return writer;
        } catch (URISyntaxException e) {
            // todo: domain URI error
            throw new RuntimeException(e);
        } catch (IOException e) {
            // todo: domain IO error
            logger.errorf(e, "Error happened while creating avro writer: %s", e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, DataFileWriter<GenericRecord> writer) {
        try {
            writer.append(event.value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commitPendingEvents(DataFileWriter<GenericRecord> writer) {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
