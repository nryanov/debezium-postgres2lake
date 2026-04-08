package io.debezium.postgres2lake.test.resource;

import io.debezium.postgres2lake.test.annotation.InjectNessieHelper;
import io.debezium.postgres2lake.test.container.NessieTestContainer;
import io.debezium.postgres2lake.test.helper.NessieHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.HashMap;
import java.util.Map;

public class NessieResource implements QuarkusTestResourceLifecycleManager {

    public static final String REF_ARG = "ref";

    private NessieTestContainer nessieTestContainer;
    private NessieHelper nessieHelper;
    private String ref;

    @Override
    public void init(Map<String, String> initArgs) {
        this.ref = initArgs.getOrDefault(REF_ARG, "main");
    }

    @Override
    public Map<String, String> start() {
        nessieTestContainer = new NessieTestContainer();
        nessieTestContainer.start();

        nessieHelper = new NessieHelper(nessieTestContainer.host());

        var properties = new HashMap<String, String>();
        properties.put("debezium.output.iceberg.properties.type", "nessie");
        properties.put("debezium.output.iceberg.properties.uri", nessieTestContainer.host());
        properties.put("debezium.output.iceberg.properties.ref", ref);
        return properties;
    }

    @Override
    public void stop() {
        nessieTestContainer.stop();
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(nessieHelper, new TestInjector.AnnotatedAndMatchesType(InjectNessieHelper.class, NessieHelper.class));
    }
}
