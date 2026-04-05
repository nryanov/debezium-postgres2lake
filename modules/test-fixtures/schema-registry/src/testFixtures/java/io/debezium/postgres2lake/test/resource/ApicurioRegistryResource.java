package io.debezium.postgres2lake.test.resource;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ApicurioRegistryResource implements QuarkusTestResourceLifecycleManager {
    private static final int REGISTRY_PORT = 8080;
    private static GenericContainer<?> apicurio;

    @Override
    public Map<String, String> start() {
        if (apicurio == null) {
            apicurio = new GenericContainer<>(DockerImageName.parse("apicurio/apicurio-registry:3.0.9"))
                    .withExposedPorts(REGISTRY_PORT)
                    .withStartupTimeout(Duration.ofMinutes(2))
                    .waitingFor(Wait.forListeningPort());
            apicurio.start();
        }

        var host = apicurio.getHost();
        var port = apicurio.getMappedPort(REGISTRY_PORT);
        var registryUrl = String.format("http://%s:%d/apis/ccompat/v7", host, port);

        var properties = new HashMap<String, String>();
        properties.put("debezium.avro.properties.schema.registry.url", registryUrl);
        return properties;
    }

    @Override
    public void stop() {
        if (apicurio != null) {
            apicurio.stop();
        }
    }
}
