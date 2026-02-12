package io.debezium.postgres2lake.common;

public interface UnsafeFunction<E extends Exception> {
    void run() throws E;
}
