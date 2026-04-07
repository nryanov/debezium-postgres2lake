package io.debezium.postgres2lake.test.container;

import org.projectnessie.testing.nessie.ImmutableNessieConfig;
import org.projectnessie.testing.nessie.NessieContainer;

public final class NessieTestContainer {
    private final NessieContainer nessie = new NessieContainer(
            ImmutableNessieConfig
                    .builder()
                    .dockerImage("ghcr.io/projectnessie/nessie")
                    .dockerTag("0.104.3")
                    .build()
    );

    public NessieTestContainer() {
    }

    public void start() {
        nessie.start();
    }

    public void stop() {
        nessie.stop();
    }

    public int port() {
        return nessie.getExternalNessiePort();
    }

    public String host() {
        return nessie.getExternalNessieUri().toString();
    }
}
