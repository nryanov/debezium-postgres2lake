package io.debezium.postgres2lake.infrastucture.s3;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class S3AvroEventSaverTest {
    @Test
    public void successfullyReadAvro() {
        var spark = SparkSession
                .builder()
                .config("spark.ui.enabled", false)
                .master("local[1]")
                .appName("development")
                .getOrCreate();

        System.out.println(spark.version());
    }
}
