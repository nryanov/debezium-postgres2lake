package io.debezium.postgres2lake.orc.infrastructure;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.orc.infrastructure.profile.OrcOutputFormatProfile;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.helper.SchemaRolloverTestQueries;
import io.debezium.postgres2lake.test.helper.WriterRolloverAssertions;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OrcOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "orc")
})
public class S3OrcSchemaRolloverTest {

    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";

    static {
        var data = GenericData.get();
        data.addLogicalTypeConversion(new Conversions.DecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new TimeConversions.DateConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    }

    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    @BeforeEach
    void cleanup() {
        eventSaver.flush();
        minioHelper.clearBucket(BUCKET);
    }

    @Test
    void schemaChangeInOneTransactionCreatesTwoOrcFiles() {
        var table = "public.test_schema_rollover_orc";
        postgresHelper.executeSql(SchemaRolloverTestQueries.createMinimalSchemaRolloverTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(SchemaRolloverTestQueries.schemaRolloverTransaction(table));

        var saver = (AbstractEventSaver<?>) eventSaver;
        WriterRolloverAssertions.awaitAndFlush(eventSaver, saver);

        WriterRolloverAssertions.assertAtLeastTwoDataFiles(
                minioHelper,
                BUCKET,
                "default/public/test_schema_rollover_orc/",
                ".orc",
                "Expected at least two ORC files after schema rollover");
    }
}
