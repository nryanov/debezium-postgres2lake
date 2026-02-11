package io.debezium.postgres2lake.engine;

import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.postgres2lake.engine.avro.AvroBinaryConverter;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class DebeziumEngineFactory {
    private static final Logger logger = Logger.getLogger(DebeziumEngineFactory.class);

    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    public void initialize() {
        var properties = properties();
        engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying((records, committer) -> {
                    records.forEach(record -> {
                        logger.infof("Record: %s", record.value());
                        try {
                            committer.markProcessed(record);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    committer.markBatchFinished();
                })
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
        properties.setProperty("name", "development");
        properties.setProperty("connector.class", PostgresConnector.class.getName());
        properties.setProperty("database.hostname", "localhost");
        properties.setProperty("database.dbname", "postgres");
        properties.setProperty("database.port", "5432");
        properties.setProperty("database.user", "postgres");
        properties.setProperty("database.password", "postgres");
        properties.setProperty("publication.name", "debezium");
        properties.setProperty("slot.name", "debezium");
        properties.setProperty("plugin.name", "pgoutput");
        properties.setProperty("snapshot.mode", "NO_DATA");
        properties.setProperty("topic.prefix", "default");

        // offset storage
        properties.setProperty("offset.storage", MemoryOffsetBackingStore.class.getName());
        // avro
        properties.setProperty("key.converter.delegate.converter.type", AvroBinaryConverter.class.getName());
        properties.setProperty("value.converter.delegate.converter.type", AvroBinaryConverter.class.getName());

        return properties;
    }
}
