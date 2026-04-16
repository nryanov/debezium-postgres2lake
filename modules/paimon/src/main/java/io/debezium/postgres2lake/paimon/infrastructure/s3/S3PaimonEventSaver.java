package io.debezium.postgres2lake.paimon.infrastructure.s3;

import io.debezium.postgres2lake.paimon.config.PaimonConfiguration;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.PaimonEventAppender;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.s3.exceptions.S3PaimonTableAccessException;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.PaimonTableWriter;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.ddl.PaimonTableDdl;
import io.debezium.postgres2lake.core.infrastructure.schema.SchemaDiffResolver;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class S3PaimonEventSaver extends AbstractEventSaver<PaimonEventAppender> {
    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final Catalog catalog;
    private final PaimonTableDdl tableDdl;
    private final SchemaConverter<org.apache.paimon.schema.Schema> schemaConverter;
    private final SchemaDiffResolver schemaDiffResolver;

    public S3PaimonEventSaver(
            PaimonConfiguration configuration,
            SchemaConverter<Schema> schemaConverter,
            ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler
            ) {
        super(configuration.threshold(), readinessMarkerEventEmitterHandler);

        var config = new Configuration();
        configuration.fileIO().properties().forEach(config::set);

        var options = new Options();
        configuration.properties().forEach(options::set);

        var catalogContext = CatalogContext.create(options, config);
        this.catalog = CatalogFactory.createCatalog(catalogContext);

        var innerSchemaConverter = new PaimonSchemaConverter();
        this.schemaConverter = schemaConverter;

        var tableSpecs = new HashMap<String, Map<String, String>>();
        configuration.tableSpecs().forEach((key, specs) -> tableSpecs.put(key, specs.properties()));

        this.tableDdl = new PaimonTableDdl(
                catalog,
                innerSchemaConverter,
                configuration.defaultTableProperties(),
                tableSpecs
        );
        this.schemaDiffResolver = new SchemaDiffResolver();
    }

    @Override
    protected PaimonEventAppender createEventAppender(EventRecord event) {
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var paimonSchema = schemaConverter.extractSchema(event);
        tableDdl.createTableIfNotExists(tableIdentifier, paimonSchema);

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();
            var tableWriter = new PaimonTableWriter(
                    tableIdentifier,
                    event.destination(),
                    table,
                    paimonSchema,
                    event.valueSchema(),
                    writerBuilder,
                    new AtomicReference<>(),
                    new ArrayList<>(),
                    new AtomicInteger(0)
            );

            return new PaimonEventAppender(tableWriter);
        } catch (Catalog.TableNotExistException e) {
            logger.errorf("Paimon table not found after createTableIfNotExists: %s", tableIdentifier);
            throw new S3PaimonTableAccessException("Paimon table not found after createTableIfNotExists: " + tableIdentifier, e);
        }
    }

    @Override
    protected void handleSchemaChanges(PaimonEventAppender appender, EventRecord event) {
        var diff = schemaDiffResolver.resolveDiff(appender.currentSchema(), event.valueSchema());
        tableDdl.handleSchemaEvolution(appender.identifier(), diff);
    }
}

