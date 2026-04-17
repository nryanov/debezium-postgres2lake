# Debezium configuration

This page contains shared Debezium-related configuration used by all format modules.

## Configuration model

- `debezium.engine.*` is passed to the embedded Debezium engine
- `debezium.avro.format` selects how Avro payloads are serialized/deserialized
- `debezium.avro.properties.*` passes serializer/deserializer properties (mainly for Confluent mode)

## Minimal example

```properties
debezium.engine.name=examples
debezium.engine.database.hostname=postgres
debezium.engine.database.dbname=postgres
debezium.engine.database.port=5432
debezium.engine.database.user=postgres
debezium.engine.database.password=postgres
debezium.engine.publication.name=debezium
debezium.engine.slot.name=debezium
debezium.engine.plugin.name=pgoutput
debezium.engine.snapshot.mode=NO_DATA
debezium.engine.topic.prefix=default
debezium.engine.offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore
```

## Important Debezium engine keys

| Property                                                                      | Why it matters                                                                    |
|-------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| `debezium.engine.database.hostname` / `port` / `dbname` / `user` / `password` | Source PostgreSQL connection used for CDC ingestion.                              |
| `debezium.engine.publication.name`                                            | PostgreSQL publication that determines which table changes are exposed.           |
| `debezium.engine.slot.name`                                                   | Replication slot consumed by Debezium; protects ordered change consumption state. |
| `debezium.engine.plugin.name`                                                 | Postgres decoding plugin (usually `pgoutput`).                                    |
| `debezium.engine.snapshot.mode`                                               | Controls initial snapshot behavior (`NO_DATA` streams only new changes).          |
| `debezium.engine.topic.prefix`                                                | Logical namespace used by Debezium records.                                       |
| `debezium.engine.offset.storage`                                              | Offset storage backend for resume behavior and restarts.                          |

## Avro serialization mode

`debezium.avro.format` supports two modes:

### `CONFLUENT`

Use Confluent serializer/deserializer and Schema Registry.

```properties
debezium.avro.format=CONFLUENT
debezium.avro.properties.schema.registry.url=http://schema-registry:8080/apis/ccompat/v7
```

Use this mode when you need registry-backed schema management and compatibility workflows.

### `BINARY`

Use internal binary Avro conversion without external Schema Registry dependency.

```properties
debezium.avro.format=BINARY
```

Use this mode for simpler local/self-contained setups or when registry integration is not required.

## Related docs

- [Features](features.md)
- [Schema evolution](schema-evolution.md)
- [Avro format](formats/avro.md)
- [Parquet format](formats/parquet.md)
- [ORC format](formats/orc.md)
- [Iceberg format](formats/iceberg.md)
- [Paimon format](formats/paimon.md)
