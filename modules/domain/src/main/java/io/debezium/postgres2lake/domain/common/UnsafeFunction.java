package io.debezium.postgres2lake.domain.common;

@FunctionalInterface
public interface UnsafeFunction<E extends Exception> {
    void run() throws E;
}
