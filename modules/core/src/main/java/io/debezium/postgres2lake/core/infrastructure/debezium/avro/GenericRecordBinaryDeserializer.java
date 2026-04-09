package io.debezium.postgres2lake.core.infrastructure.debezium.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class GenericRecordBinaryDeserializer implements GenericRecordDeserializer {
    @Override
    public GenericRecord deserializeValue(String topic, byte[] data) {
        return deserialize(data);
    }

    @Override
    public GenericRecord deserializeKey(String topic, byte[] data) {
        return deserialize(data);
    }

    private GenericRecord deserialize(byte[] data) {
        try {
            var bin = new SeekableByteArrayInput(data);
            var reader = new DataFileReader<GenericRecord>(bin, new GenericDatumReader<>());
            return reader.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
