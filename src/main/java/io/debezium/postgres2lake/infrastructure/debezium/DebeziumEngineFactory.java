package io.debezium.postgres2lake.infrastructure.debezium;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Binary;
import io.debezium.postgres2lake.infrastructure.serde.avro.AvroBinaryConverter;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class DebeziumEngineFactory {
    private static final Logger logger = Logger.getLogger(DebeziumEngineFactory.class);

    private DebeziumEngine<ChangeEvent<Object, Object>> engine;
    private ExecutorService executor;

    private final DebeziumEventConsumer eventConsumer;
    private final DebeziumConfiguration configuration;

    public DebeziumEngineFactory(DebeziumEventConsumer eventConsumer, DebeziumConfiguration configuration) {
        this.eventConsumer = eventConsumer;
        this.configuration = configuration;
    }

    public void initialize() {
        var properties = properties();
        engine = DebeziumEngine.create(Binary.class)
                .using(properties)
                .notifying(eventConsumer)
                .build();

        executor = Executors.newSingleThreadExecutor();
    }

    public void start(@Observes StartupEvent ev) {
        logger.infof("Starting debezium");
        initialize();
        executor.execute(engine);
        logger.infof("Debezium has successfully started");
    }

    public void stop(@Observes ShutdownEvent ev) {
        try {
            engine.close();
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.info("Waiting another 5 seconds for the embedded engine to shut down");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            logger.warnf(e, "Error happened while closing engine: %s", e.getLocalizedMessage());
        }
    }

    private Properties properties() {
        var properties = new Properties();
        configuration.engine().forEach(properties::setProperty);

        // avro
        switch (configuration.avro()) {
            case CONFLUENT -> {}
            case BINARY -> {
                properties.setProperty("key.converter.delegate.converter.type", AvroBinaryConverter.class.getName());
                properties.setProperty("value.converter.delegate.converter.type", AvroBinaryConverter.class.getName());
            }
        }
        // connector
        properties.setProperty("connector.class", PostgresConnector.class.getName());

        return properties;
    }
}
