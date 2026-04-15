# SPI: Commit Event Emitter (Kafka)

Kafka commit emitter publishes a message after file commit events.

Provider class:
`io.debezium.postgres2lake.extensions.commit.event.emitter.kafka.KafkaCommitEventEmitterProvider`

API module: `extensions/commit-event-emitter-api`

## Configuration example

```properties
debezium.extensions.commit-event-emitter.name=io.debezium.postgres2lake.extensions.commit.event.emitter.kafka.KafkaCommitEventEmitterProvider

debezium.extensions.commit-event-emitter.properties.topic=postgres2lake.commits
debezium.extensions.commit-event-emitter.properties.bootstrap.servers=kafka:9092
debezium.extensions.commit-event-emitter.properties.acks=all
debezium.extensions.commit-event-emitter.properties.compression.type=zstd
debezium.extensions.commit-event-emitter.properties.linger.ms=5
```

## Available configs

| Property | Required | Description | Values / examples |
|---|---|---|---|
| `debezium.extensions.commit-event-emitter.name` | Yes | SPI provider class name | `...KafkaCommitEventEmitterProvider` |
| `debezium.extensions.commit-event-emitter.properties.topic` | Yes | Topic name for commit events | `postgres2lake.commits` |
| `debezium.extensions.commit-event-emitter.properties.*` | No | Kafka producer properties map, passed through to `KafkaProducer` | `bootstrap.servers`, `acks`, `compression.type`, `linger.ms` |
