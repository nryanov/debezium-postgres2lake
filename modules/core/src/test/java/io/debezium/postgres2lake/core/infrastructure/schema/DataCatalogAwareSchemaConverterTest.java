package io.debezium.postgres2lake.core.infrastructure.schema;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableColumnType;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableField;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.debezium.postgres2lake.test.avro.AvroTestFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

public class DataCatalogAwareSchemaConverterTest {

    @Test
    void extractSchemaWhenNotNewSchemaDoesNotCallCatalog() {
        var delegate = new RecordingSchemaConverter<>("ok");
        delegate.setNewSchema(false);
        var catalog = new RecordingDataCatalogHandler();
        var converter = new DataCatalogAwareSchemaConverter<>(delegate, catalog);
        var keySchema = record("Key", List.of(field("id", required(Schema.Type.LONG))));
        var valueSchema = record("Val", List.of(field("id", required(Schema.Type.LONG))));
        var event = insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema));

        converter.extractSchema(event);

        assertEquals(0, catalog.createOrUpdateCount());
        assertEquals(1, delegate.extractSchemaCallCount());
    }

    @Test
    void extractSchemaWhenNewSchemaCallsCatalogWithExpectedTableSchema() {
        var delegate = new RecordingSchemaConverter<>("ok");
        var catalog = new RecordingDataCatalogHandler();
        var converter = new DataCatalogAwareSchemaConverter<>(delegate, catalog);
        var meta = record("Meta", List.of(field("flag", required(Schema.Type.BOOLEAN))));
        var keySchema = record("Key", List.of(field("id", required(Schema.Type.LONG))));
        var valueSchema = record(
                "Val",
                List.of(
                        field("id", required(Schema.Type.LONG)),
                        field("title", required(Schema.Type.STRING)),
                        new Schema.Field(
                                "note",
                                nullable(required(Schema.Type.STRING)),
                                null,
                                JsonProperties.NULL_VALUE),
                        field("meta", meta)));
        var event = insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema));

        var expected = new TableSchema(
                List.of(
                        new TableField(
                                "id",
                                (String) null,
                                new TableColumnType.Long(TableColumnType.TableColumnConstraint.PRIMARY_KEY)),
                        new TableField(
                                "title",
                                (String) null,
                                new TableColumnType.Text(TableColumnType.TableColumnConstraint.REQUIRED)),
                        new TableField(
                                "note",
                                (String) null,
                                new TableColumnType.Text(TableColumnType.TableColumnConstraint.OPTIONAL)),
                        new TableField(
                                "meta",
                                (String) null,
                                new TableColumnType.Record(
                                        TableColumnType.TableColumnConstraint.REQUIRED,
                                        List.of(
                                                new TableField(
                                                        "flag",
                                                        (String) null,
                                                        new TableColumnType.Boolean(
                                                                TableColumnType.TableColumnConstraint.REQUIRED)))))));

        converter.extractSchema(event);

        assertEquals(1, catalog.createOrUpdateCount());
        assertEquals(1, delegate.extractSchemaCallCount());
        assertEquals(new TableDestination("db", "schema", "table"), catalog.lastDestination());
        assertEquals(expected, catalog.lastSchema());
    }

    @Test
    void extractSchemaWhenCatalogThrowsStillCallsDelegate() {
        var delegate = new RecordingSchemaConverter<>("ok");
        var catalog = new RecordingDataCatalogHandler();
        catalog.setFailOnCreateOrUpdate(new RuntimeException("catalog down"));
        var converter = new DataCatalogAwareSchemaConverter<>(delegate, catalog);
        var keySchema = record("Key", List.of(field("id", required(Schema.Type.LONG))));
        var valueSchema = record("Val", List.of(field("id", required(Schema.Type.LONG))));
        var event = insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema));

        assertEquals("ok", converter.extractSchema(event));

        assertEquals(1, catalog.createOrUpdateCount());
        assertEquals(1, delegate.extractSchemaCallCount());
    }

    @Test
    void extractSchemaWhenValueConversionFailsDoesNotCallCatalog() {
        var delegate = new RecordingSchemaConverter<>("ok");
        var catalog = new RecordingDataCatalogHandler();
        var converter = new DataCatalogAwareSchemaConverter<>(delegate, catalog);
        var keySchema = record("Key", List.of());
        var brokenValue = record("Val", List.of(field("x", Schema.create(Schema.Type.NULL))));
        var event = insertEvent(emptyRecord(keySchema), emptyRecord(brokenValue));

        converter.extractSchema(event);

        assertEquals(0, catalog.createOrUpdateCount());
        assertEquals(1, delegate.extractSchemaCallCount());
    }

    private static final class RecordingSchemaConverter<T> implements SchemaConverter<T> {

        private final T extractResult;
        private boolean newSchema = true;
        private final List<EventRecord> extractSchemaCalls = new ArrayList<>();

        public RecordingSchemaConverter(T extractResult) {
            this.extractResult = extractResult;
        }

        public void setNewSchema(boolean newSchema) {
            this.newSchema = newSchema;
        }

        public int extractSchemaCallCount() {
            return extractSchemaCalls.size();
        }

        @Override
        public T extractSchema(EventRecord event) {
            extractSchemaCalls.add(event);
            return extractResult;
        }

        @Override
        public boolean isNewSchema(EventRecord event) {
            return newSchema;
        }
    }

    private static final class RecordingDataCatalogHandler implements DataCatalogHandler {

        private final AtomicInteger createOrUpdateCount = new AtomicInteger();
        private volatile TableDestination lastDestination;
        private volatile TableSchema lastSchema;
        private volatile RuntimeException failOnCreateOrUpdate;

        public void setFailOnCreateOrUpdate(RuntimeException failOnCreateOrUpdate) {
            this.failOnCreateOrUpdate = failOnCreateOrUpdate;
        }

        public int createOrUpdateCount() {
            return createOrUpdateCount.get();
        }

        public TableDestination lastDestination() {
            return lastDestination;
        }

        public TableSchema lastSchema() {
            return lastSchema;
        }

        @Override
        public void initialize(Map<String, String> properties) {
            // no-op
        }

        @Override
        public void createOrUpdateTable(TableDestination destination, TableSchema schema) {
            createOrUpdateCount.incrementAndGet();
            lastDestination = destination;
            lastSchema = schema;
            var fail = failOnCreateOrUpdate;
            if (fail != null) {
                throw fail;
            }
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
