package io.debezium.postgres2lake.extensions.commit.event.emitter.kafka;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.common.SpiPropertyReader;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class KafkaCommitEventEmitterHandler implements CommitEventEmitterHandler {
    private final static Logger logger = LoggerFactory.getLogger(KafkaCommitEventEmitterHandler.class);

    private String topic;
    private Producer<String, String> producer;

    @Override
    public void emit(TableDestination destination, String file) {
        try {
            var key = String.format("%s.%s.%s", destination.database(), destination.schema(), destination.table());
            producer.send(new ProducerRecord<>(topic, key, file), new KafkaCommitEventEmitterCallback());
        } catch (Exception e) {
            // handle all errors to avoid fail in the main logic
            logger.error("Unexpected error happened while emitting commit event: {}", e.getLocalizedMessage());
        }
    }

    @Override
    public void initialize(Map<String, String> properties) {
        var producerProperties = new Properties();
        producerProperties.putAll(properties);

        topic = SpiPropertyReader.required(properties, "topic");

        producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    private static class KafkaCommitEventEmitterCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                logger.trace("Successfully emit commit event");
            } else {
                logger.error("Error happened while emitting commit event: {}", exception.getLocalizedMessage());
            }
        }
    }
}
