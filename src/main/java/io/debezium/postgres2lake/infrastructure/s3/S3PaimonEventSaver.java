package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.paimon.AvroToPaimonMapper;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class S3PaimonEventSaver extends AbstractEventSaver<PaimonWriter> {
    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final Catalog catalog;
    private final AvroToPaimonMapper mapper;

    public S3PaimonEventSaver(OutputConfiguration.Threshold threshold) {
        super(threshold);

        var config = new Configuration();
        config.set("fs.s3a.access.key", "admin");
        config.set("fs.s3a.secret.key", "password");
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", "http://localhost:9000");
        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", "s3a://warehouse/paimon-warehouse");
        options.set("jdbc-url", "jdbc:postgresql://localhost:5432/postgres");
        options.set("jdbc-user", "postgres");
        options.set("jdbc-password", "postgres");
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        var catalogContext = CatalogContext.create(options, config);
        this.catalog = CatalogFactory.createCatalog(catalogContext);
        this.mapper = new AvroToPaimonMapper();
    }

    @Override
    protected PaimonWriter createWriter(EventRecord event) {
        var tableIdentifier = Identifier.create("paimon-development", "data");
        var paimonSchema = mapper.avroToPaimonSchema(event.key().getSchema(), event.value().getSchema());
        try {
            catalog.createDatabase("paimon-development", true);
            catalog.createTable(tableIdentifier, paimonSchema, true);
        } catch (Exception e) {
            logger.errorf(e, "Error happened while creating namespace/table: %s", e.getLocalizedMessage());
        }

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();

            return new PaimonWriter(table, paimonSchema, writerBuilder, new AtomicReference<>(), new ArrayList<>(), new AtomicInteger(0));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
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
        write.write(mapper.createPaimonRecord(wrapper.schema(), event.value()), bucket);
        // todo: commit only before saving data using single pending commit per batch
        var pendingCommit = write.prepareCommit(false, wrapper.commitId().incrementAndGet());
        wrapper.pendingCommits().addAll(pendingCommit);
    }

    @Override
    protected void commitPendingEvents(PaimonWriter wrapper) {
        var commit = wrapper.writeBuilder().newCommit();
        commit.commit(wrapper.commitId().get(), wrapper.pendingCommits());
        wrapper.pendingCommits().clear();
    }
}

