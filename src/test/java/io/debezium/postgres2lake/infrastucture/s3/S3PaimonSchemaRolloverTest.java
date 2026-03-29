package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.infrastucture.profile.PaimonOutputFormatProfile;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("until handleSchemaChanges implemented the Paimon table schema")
@QuarkusTest
@TestProfile(PaimonOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot"),
        @ResourceArg(name = PostgresResource.CATALOG_TYPE_ARG, value = "paimon")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "paimon")
})
public class S3PaimonSchemaRolloverTest {

    @Test
    void schemaChangeCommitsFirstBatchBeforeNewWriter() {
    }
}
