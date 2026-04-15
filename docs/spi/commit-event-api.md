# SPI API: Commit Event Emitter

Root API module: `extensions/commit-event-emitter-api`

This extension point emits events after files are committed by the pipeline.

## Configuration root

| Property pattern | Description |
|---|---|
| `debezium.extensions.commit-event-emitter.name` | Provider class name |
| `debezium.extensions.commit-event-emitter.properties.*` | Provider-specific properties map |

## Implementations

| Implementation module | Provider class | Docs |
|---|---|---|
| `extensions/commit-event-emitter-kafka` | `io.debezium.postgres2lake.extensions.commit.event.emitter.kafka.KafkaCommitEventEmitterProvider` | [Kafka](commit-event-emitter-kafka.md) |

If no provider class is set, the default no-op handler from `extensions/commit-event-emitter-api` is used.
