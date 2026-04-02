package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonEventAppender;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3PaimonTableAccessException;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonTableWriter;
import io.debezium.postgres2lake.infrastructure.format.paimon.ddl.PaimonTableDdl;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class S3PaimonEventSaver extends AbstractEventSaver<PaimonTableWriter> {
    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final Catalog catalog;
    private final PaimonTableDdl tableDdl;
    private final SchemaConverter<org.apache.paimon.schema.Schema> schemaConverter;

    public S3PaimonEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputConfiguration.Paimon paimon
    ) {
        super(threshold, new PaimonEventAppender());

        var config = new Configuration();
        paimon.fileIO().ifPresent(cfg -> cfg.properties().forEach(config::set));

        var options = new Options();
        paimon.properties().forEach(options::set);

        var catalogContext = CatalogContext.create(options, config);
        this.catalog = CatalogFactory.createCatalog(catalogContext);
        this.tableDdl = new PaimonTableDdl(catalog);
        this.schemaConverter = new PaimonSchemaConverter();
    }

    @Override
    protected PaimonTableWriter createWriter(EventRecord event) {
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var paimonSchema = schemaConverter.extractSchema(event);
        tableDdl.createTableIfNotExists(tableIdentifier, paimonSchema);

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();

            return new PaimonTableWriter(table, paimonSchema, event.valueSchema(), writerBuilder, new AtomicReference<>(), new ArrayList<>(), new AtomicInteger(0));
        } catch (Catalog.TableNotExistException e) {
            logger.errorf("\"Paimon table not found after createTableIfNotExists: %s", tableIdentifier);
            throw new S3PaimonTableAccessException("Paimon table not found after createTableIfNotExists: " + tableIdentifier, e);
        }
    }

    @Override
    protected void handleSchemaChanges(EventRecord event, Schema currentSchema) {
        // TODO: execute TABLE ALTER
    }

    @Override
    protected String resolvePartition(EventRecord event) {
        // paimon resolve partition in StreamWriteBuilder
        return "";
    }
}

