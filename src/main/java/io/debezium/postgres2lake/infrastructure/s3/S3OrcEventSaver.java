package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.orc.AvroToOrcMapper;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3WriterOpenException;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcTableWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jboss.logging.Logger;

import java.io.IOException;

public class S3OrcEventSaver extends AbstractEventSaver<OrcTableWriter> {

    private static final Logger logger = Logger.getLogger(S3OrcEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final OutputConfiguration.FileIO fileIO;
    private final OrcCompressionCodec codec;
    private final AvroToOrcMapper mapper;

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
        this.mapper = new AvroToOrcMapper();
    }

    @Override
    protected OrcTableWriter createWriter(EventRecord event) {
        var location = outputLocationGenerator.generateLocation("warehouse", event);
        var writer = createFileWriter(location, mapper.avroToOrcSchema(event.valueSchema()));
        var batch = writer.getSchema().createRowBatch(); // todo: configure batch size

        return new OrcTableWriter(writer, batch, event.valueSchema(), resolvePartition(event));
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
    protected void handleSchemaChanges(EventRecord event, Schema currentSchema) {
        // nothing to do
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        return outputLocationGenerator.getPartition("warehouse", event);
    }

    @Override
    protected void appendEvent(EventRecord event, OrcTableWriter writer) throws IOException {
        var batch = writer.batch();
        var row = batch.size;
        batch.size += 1;

        saveRecord(event.value(), writer.writer().getSchema(), row, batch);

        if (batch.size == batch.getMaxSize()) {
            writer.writer().addRowBatch(batch);
            batch.reset();
        }
    }

    @Override
    protected void commitPendingEvents(OrcTableWriter writer) throws IOException {
        if (writer.batch().size != 0) {
            logger.infof("Add rows batch: %s", writer.batch().count());
            writer.writer().addRowBatch(writer.batch());
            writer.batch().reset();
        }

        writer.writer().close();
    }

    private void saveRecord(GenericRecord record, TypeDescription schema, int rowIdx, VectorizedRowBatch vector) {
        var avroFields = record.getSchema().getFields();
        var orcFields = schema.getChildren();

        for (var fieldIdx = 0; fieldIdx < avroFields.size(); fieldIdx++) {
            var avroField = avroFields.get(fieldIdx);
            var avroValue = record.get(fieldIdx);

            var columnVector = vector.cols[fieldIdx];
            var orcField = orcFields.get(fieldIdx);

            mapper.saveValue(avroField.schema(), avroValue, orcField, rowIdx, columnVector);
        }
    }
}
