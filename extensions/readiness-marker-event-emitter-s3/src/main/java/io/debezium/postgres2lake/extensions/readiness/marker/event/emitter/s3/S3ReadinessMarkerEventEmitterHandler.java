package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3;

import io.debezium.postgres2lake.extensions.common.SpiPropertyReader;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class S3ReadinessMarkerEventEmitterHandler implements ReadinessMarkerEventEmitterHandler {
    private static final Logger logger = LoggerFactory.getLogger(S3ReadinessMarkerEventEmitterHandler.class);

    private String bucket;
    private String markerKey;
    private String markerKeyPrefix;
    private S3Client s3Client;

    @Override
    public void initialize(Map<String, String> properties) {
        bucket = SpiPropertyReader.required(properties, "bucket");
        markerKey = SpiPropertyReader.optional(properties, "marker-key", "");
        markerKeyPrefix = normalizePrefix(SpiPropertyReader.optional(properties, "marker-key-prefix", "readiness-markers"));
    }

    @Override
    public void emit(List<TableDestination> destinations) {
        try {
            var commitTimestamp = Instant.now();
            var key = resolveKey(commitTimestamp);
            var payload = buildPayload(commitTimestamp, destinations);
            if (s3Client == null) {
                s3Client = S3Client.create();
            }

            var request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType("application/json")
                    .build();

            s3Client.putObject(request, RequestBody.fromString(payload, StandardCharsets.UTF_8));
            logger.trace("Successfully emitted readiness marker to s3://{}/{}", bucket, key);
        } catch (Exception e) {
            // handle all errors to avoid fail in the main logic
            logger.error("Unexpected error happened while emitting readiness marker: {}", e.getLocalizedMessage());
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    String resolveKey(Instant commitTimestamp) {
        if (!markerKey.isBlank()) {
            return markerKey;
        }

        var suffix = DateTimeFormatter.ISO_INSTANT.format(commitTimestamp)
                .replace(":", "-");
        return markerKeyPrefix + "/" + suffix + ".json";
    }

    private String normalizePrefix(String prefix) {
        if (prefix.endsWith("/")) {
            return prefix.substring(0, prefix.length() - 1);
        }
        return prefix;
    }

    String buildPayload(Instant commitTimestamp, List<TableDestination> destinations) {
        var destinationsPayload = destinations.stream()
                .map(this::toJsonObject)
                .reduce((left, right) -> left + "," + right)
                .orElse("");

        return "{"
                + "\"commitTimestamp\":\"" + escape(commitTimestamp.toString()) + "\","
                + "\"destinations\":[" + destinationsPayload + "]"
                + "}";
    }

    private String toJsonObject(TableDestination destination) {
        return "{"
                + "\"database\":\"" + escape(destination.database()) + "\","
                + "\"schema\":\"" + escape(destination.schema()) + "\","
                + "\"table\":\"" + escape(destination.table()) + "\""
                + "}";
    }

    private String escape(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
