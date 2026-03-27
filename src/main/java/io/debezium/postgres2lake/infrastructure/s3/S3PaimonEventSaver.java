package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.paimon.AvroToPaimonMapper;
import io.debezium.postgres2lake.infrastructure.s3.exceptions.S3PaimonTableAccessException;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonWriter;
import io.debezium.postgres2lake.infrastructure.format.paimon.ddl.PaimonTableDdl;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class S3PaimonEventSaver extends AbstractEventSaver<PaimonWriter> {
    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final Catalog catalog;
    private final PaimonTableDdl tableDdl;
    private final AvroToPaimonMapper mapper;

    public S3PaimonEventSaver(
            OutputConfiguration.Threshold threshold,
            OutputConfiguration.Paimon paimon
    ) {
        super(threshold);

        var config = new Configuration();
        paimon.fileIO().ifPresent(cfg -> cfg.properties().forEach(config::set));

        var options = new Options();
        paimon.properties().forEach(options::set);

        var catalogContext = CatalogContext.create(options, config);
        this.catalog = CatalogFactory.createCatalog(catalogContext);
        this.tableDdl = new PaimonTableDdl(catalog);
        this.mapper = new AvroToPaimonMapper();
    }

    @Override
    protected PaimonWriter createWriter(EventRecord event) {
        var tableIdentifier = tableDdl.tableIdentifier(event);
        var paimonSchema = mapper.avroToPaimonSchema(event.key().getSchema(), event.value().getSchema());
        tableDdl.createTableIfNotExists(tableIdentifier, paimonSchema);

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();

            return new PaimonWriter(table, paimonSchema, writerBuilder, new AtomicReference<>(), new ArrayList<>(), new AtomicInteger(0));
        } catch (Catalog.TableNotExistException e) {
            logger.errorf("\"Paimon table not found after createTableIfNotExists: %s", tableIdentifier);
            throw new S3PaimonTableAccessException("Paimon table not found after createTableIfNotExists: " + tableIdentifier, e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, PaimonWriter wrapper) throws Exception {
        var write = wrapper.writer().get();
        if (write == null) {
            write = wrapper.writeBuilder().newWrite();
            wrapper.writer().set(write);
        }

        // todo: fix bucket id resolution
        var bucket = 0;
        write.write(mapper.createPaimonRecord(wrapper.schema(), event), bucket);
    }

    @Override
    protected void commitPendingEvents(PaimonWriter wrapper) throws Exception {
        var write = wrapper.writer().get();
        var pendingCommit = write.prepareCommit(false, wrapper.commitId().incrementAndGet());
        wrapper.pendingCommits().addAll(pendingCommit);

        var commit = wrapper.writeBuilder().newCommit();
        commit.commit(wrapper.commitId().get(), wrapper.pendingCommits());
        wrapper.pendingCommits().clear();
    }
}

