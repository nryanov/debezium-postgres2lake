package io.debezium.postgres2lake.core.infrastructure.debezium.avro;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Used as debezium `converter.delegate.converter.type`
 */
public class AvroBinaryConverter implements Converter {
    private AvroData avroData;

    @Override
    public void configure(Map<String, ?> configs, boolean b) {
        avroData = new AvroData(new AvroDataConfig(configs));
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return fromConnectData(topic, null, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        try {
            var avroSchema = avroData.fromConnectSchema(schema);
            var avroObject = (GenericRecord) avroData.fromConnectData(schema, value);

            var bout = new ByteArrayOutputStream();
            var writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(avroSchema)).create(avroSchema, bout);
            writer.append(avroObject);
            writer.flush();

            return bout.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return toConnectData(topic, null, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        try {
            var bin = new SeekableByteArrayInput(value);
            var reader = new DataFileReader<GenericRecord>(bin, new GenericDatumReader<>());
            var parsedRecord = reader.next();

            return avroData.toConnectData(parsedRecord.getSchema(), parsedRecord);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
