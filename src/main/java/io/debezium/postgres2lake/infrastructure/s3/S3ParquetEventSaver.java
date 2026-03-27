package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3InvalidOutputUriException;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3WriterOpenException;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class S3ParquetEventSaver extends AbstractEventSaver<ParquetWriter<GenericRecord>> {
    private static final Logger logger = Logger.getLogger(S3ParquetEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;
    private final ParquetCompressionCodec compressionCodec;

    public S3ParquetEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputLocationGenerator outputLocationGenerator,
            OutputConfiguration.FileIO fileIO,
            ParquetCompressionCodec compressionCodec
    ) {
        super(threshold);
        this.outputLocationGenerator = outputLocationGenerator;
        this.fileIO = fileIO;
        this.compressionCodec = compressionCodec;
    }

    @Override
    protected ParquetWriter<GenericRecord> createWriter(EventRecord event) {
        var location = outputLocationGenerator.generateLocation("warehouse", event);
        try {
            logger.infof("Opening parquet writer for `%s`", location);
            var path = new Path(new URI(location));
            var config = new Configuration();
            fileIO.properties().forEach(config::set);

            var builder = AvroParquetWriter
                    .<GenericRecord>builder(HadoopOutputFile.fromPath(path, config))
                    .withCompressionCodec(compressionCodec.codecName)
                    .withSchema(event.value().getSchema());
            var writer = builder.build();

            logger.infof("Successfully opened writer for `%s`", location);

            return writer;
        } catch (URISyntaxException e) {
            logger.errorf("Invalid output URI: %s", location);
            throw new S3InvalidOutputUriException("Invalid output URI: " + location, e);
        } catch (IOException e) {
            logger.errorf(e, "Error happened while creating parquet writer: %s", e.getLocalizedMessage());
            throw new S3WriterOpenException("Failed to open Parquet writer for: " + location, e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, ParquetWriter<GenericRecord> writer) throws IOException {
        writer.write(event.value());
    }

    @Override
    protected void commitPendingEvents(ParquetWriter<GenericRecord> writer) throws IOException {
        writer.close();
    }
}
