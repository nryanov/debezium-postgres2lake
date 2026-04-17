package io.debezium.postgres2lake.orc.bootstrap;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.NoOpCommitEventEmitterHandler;
import io.debezium.postgres2lake.orc.config.OrcConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.orc.infrastructure.OrcCompressionCodec;
import io.debezium.postgres2lake.orc.infrastructure.OrcSchemaConverter;
import io.debezium.postgres2lake.orc.infrastructure.OrcEventSaver;
import io.debezium.postgres2lake.core.bootstrap.OutputLocationGeneratorFactory;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
import io.debezium.postgres2lake.orc.infrastructure.appender.CommitEventEmmitterAwareOrcEventAppenderFactory;
import io.debezium.postgres2lake.orc.infrastructure.appender.DefaultOrcEventAppenderFactory;
import io.debezium.postgres2lake.orc.infrastructure.appender.OrcEventAppenderFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.orc.TypeDescription;

@ApplicationScoped
public class OrcBeans {
    private final static int DEFAULT_ROW_BATCH_SIZE = 1024;

    @Inject
    OrcConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Inject
    CommitEventEmitterHandler commitEventEmitterHandler;

    @Inject
    ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), OutputFileFormat.orc);

        var schemaConverter = resolveSchemaConverter();
        var appenderFactory = resolveAppenderFactory();

        return new OrcEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(OrcCompressionCodec.NONE),
                schemaConverter,
                appenderFactory,
                readinessMarkerEventEmitterHandler,
                configuration.rowBatchSize().filter(it -> it > 0).orElse(DEFAULT_ROW_BATCH_SIZE)
        );
    }

    private SchemaConverter<TypeDescription> resolveSchemaConverter() {
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            return new CachedSchemaConverter<>(new OrcSchemaConverter());
        } else {
            return new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new OrcSchemaConverter()),
                    dataCatalogHandler
            );
        }
    }

    private OrcEventAppenderFactory resolveAppenderFactory() {
        if (commitEventEmitterHandler instanceof NoOpCommitEventEmitterHandler) {
            return new DefaultOrcEventAppenderFactory();
        } else {
            return new CommitEventEmmitterAwareOrcEventAppenderFactory(commitEventEmitterHandler);
        }
    }
}
