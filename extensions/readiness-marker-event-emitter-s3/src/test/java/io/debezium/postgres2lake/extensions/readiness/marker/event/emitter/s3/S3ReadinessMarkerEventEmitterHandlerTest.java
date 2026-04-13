package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3;

import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3ReadinessMarkerEventEmitterHandlerTest {

    @Test
    void resolveKeyUsesPrefixWhenMarkerKeyIsNotSpecified() {
        var handler = new S3ReadinessMarkerEventEmitterHandler();
        handler.initialize(Map.of(
                "bucket", "warehouse",
                "marker-key-prefix", "custom-prefix/"
        ));

        var key = handler.resolveKey(Instant.parse("2026-04-14T11:12:13Z"));

        assertEquals("custom-prefix/2026-04-14T11-12-13Z.json", key);
        handler.close();
    }

    @Test
    void resolveKeyUsesStaticMarkerKeyWhenSpecified() {
        var handler = new S3ReadinessMarkerEventEmitterHandler();
        handler.initialize(Map.of(
                "bucket", "warehouse",
                "marker-key", "markers/ready.json"
        ));

        var key = handler.resolveKey(Instant.parse("2026-04-14T11:12:13Z"));

        assertEquals("markers/ready.json", key);
        handler.close();
    }

    @Test
    void buildPayloadContainsTimestampAndDestinations() {
        var handler = new S3ReadinessMarkerEventEmitterHandler();
        var payload = handler.buildPayload(
                Instant.parse("2026-04-14T11:12:13Z"),
                List.of(
                        new TableDestination("db1", "schema1", "table1"),
                        new TableDestination("db2", "schema2", "table2")
                )
        );

        assertTrue(payload.contains("\"commitTimestamp\":\"2026-04-14T11:12:13Z\""));
        assertTrue(payload.contains("\"database\":\"db1\""));
        assertTrue(payload.contains("\"schema\":\"schema1\""));
        assertTrue(payload.contains("\"table\":\"table1\""));
        assertTrue(payload.contains("\"database\":\"db2\""));
        assertTrue(payload.contains("\"schema\":\"schema2\""));
        assertTrue(payload.contains("\"table\":\"table2\""));
    }
}
