package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.infrastructure.debezium.DebeziumConfiguration;
import io.debezium.postgres2lake.infrastructure.debezium.avro.GenericRecordBinaryDeserializer;
import io.debezium.postgres2lake.infrastructure.debezium.avro.GenericRecordConfluentDeserializer;
import io.debezium.postgres2lake.infrastructure.debezium.avro.UnwrappedEventRecordDeserializer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class CoreBeans {
    @Inject
    DebeziumConfiguration debeziumConfiguration;

    @Singleton
    @Produces
    UnwrappedEventRecordDeserializer unwrappedEventRecordDeserializer() {
        var avro = debeziumConfiguration.avro();

        var genericDeserializer = switch (avro.format()) {
            case CONFLUENT -> new GenericRecordConfluentDeserializer(avro.properties());
            case BINARY -> new GenericRecordBinaryDeserializer();
        };

        return new UnwrappedEventRecordDeserializer(genericDeserializer);
    }
}
