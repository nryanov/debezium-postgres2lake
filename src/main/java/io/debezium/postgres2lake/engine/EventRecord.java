package io.debezium.postgres2lake.engine;

import org.apache.avro.generic.GenericRecord;

public record EventRecord(GenericRecord key, GenericRecord value, String destination) {}
