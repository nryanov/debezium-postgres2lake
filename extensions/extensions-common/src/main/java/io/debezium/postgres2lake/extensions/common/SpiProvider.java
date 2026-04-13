package io.debezium.postgres2lake.extensions.common;

public interface SpiProvider<H> {
    H create();
}
