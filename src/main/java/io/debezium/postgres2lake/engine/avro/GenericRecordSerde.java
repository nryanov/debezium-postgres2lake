package io.debezium.postgres2lake.engine.avro;

import io.debezium.engine.ChangeEvent;
import io.debezium.postgres2lake.engine.EventRecord;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

@ApplicationScoped
public class GenericRecordSerde {
    public EventRecord deserialize(ChangeEvent<Object, Object> event) {
        var keyPart = (byte[]) event.key();
        var valuePart = (byte[]) event.value();

        return new EventRecord(deserialize(keyPart), deserialize(valuePart), event.destination());
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
