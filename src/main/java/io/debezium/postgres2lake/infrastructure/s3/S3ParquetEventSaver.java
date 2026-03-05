package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class S3ParquetEventSaver extends AbstractEventSaver<ParquetWriter<GenericRecord>> {
    private static final Logger logger = Logger.getLogger(S3ParquetEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;

    public S3ParquetEventSaver(OutputLocationGenerator outputLocationGenerator, OutputConfiguration.FileIO fileIO) {
        super();
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
    }

    @Override
    protected ParquetWriter<GenericRecord> createWriter(EventRecord event) {
        try {
            var location = outputLocationGenerator.generateLocation("warehouse", event);
            logger.infof("Opening parquet writer for `%s`", location);
            var path = new Path(new URI(location));
            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var builder = AvroParquetWriter
                    .<GenericRecord>builder(HadoopOutputFile.fromPath(path, config))
                    // todo: allow to setup codec
                    .withCompressionCodec(CompressionCodecName.ZSTD)
                    .withSchema(event.value().getSchema());
            var writer = builder.build();

            logger.infof("Successfully opened writer for `%s`", location);

            return writer;
        } catch (URISyntaxException e) {
            // todo: domain URI error
            throw new RuntimeException(e);
        } catch (IOException e) {
            // todo: domain IO error
            logger.errorf(e, "Error happened while creating parquet writer: %s", e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, ParquetWriter<GenericRecord> writer) {
        try {
            writer.write(event.value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commitPendingEvents(ParquetWriter<GenericRecord> writer) {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
