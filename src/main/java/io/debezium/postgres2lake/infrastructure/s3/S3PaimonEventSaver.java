package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonWriter;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class S3PaimonEventSaver extends AbstractEventSaver<PaimonWriter> {
    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final Catalog catalog;

    public S3PaimonEventSaver() {
        super();

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
    }

    @Override
    protected PaimonWriter createWriter(EventRecord event) {
        var tableIdentifier = Identifier.create("paimon-development", "data");
        try {
            var paimonSchema = org.apache.paimon.schema.Schema
                    .newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

            catalog.createDatabase("paimon-development", true);
            catalog.createTable(tableIdentifier, paimonSchema, true);
        } catch (Exception e) {
            logger.errorf(e, "Error happened while creating namespace/table: %s", e.getLocalizedMessage());
        }

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();

            return new PaimonWriter(table, writerBuilder, new AtomicReference<>(), new ArrayList<>(), new AtomicInteger(0));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void appendEvent(EventRecord event, PaimonWriter wrapper) {
        try {
            var write = wrapper.writer().get();
            if (write == null) {
                write = wrapper.writeBuilder().newWrite();
                wrapper.writer().set(write);
            }

            // todo: fix bucket id resolution
            var bucket = 0;
            write.write(GenericRow.ofKind(RowKind.INSERT, event.value().get("id")), bucket);
            // todo: commit only before saving data
            var pendingCommit = write.prepareCommit(false, wrapper.commitId().incrementAndGet());
            wrapper.pendingCommits().addAll(pendingCommit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void commitPendingEvents(PaimonWriter wrapper) {
        var commit = wrapper.writeBuilder().newCommit();
        commit.commit(wrapper.commitId().get(), wrapper.pendingCommits());
        wrapper.pendingCommits().clear();
    }
}

