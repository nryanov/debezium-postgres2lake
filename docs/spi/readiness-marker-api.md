# SPI API: Readiness Marker Emitter

Root API module: `extensions/readiness-marker-event-emitter-api`

This extension point emits readiness markers after commit cycles, which can be used as downstream triggers.

## Configuration root

| Property pattern | Description |
|---|---|
| `debezium.extensions.readiness-marker-event-emitter.name` | Provider class name |
| `debezium.extensions.readiness-marker-event-emitter.properties.*` | Provider-specific properties map |

## Implementations

| Implementation module | Provider class | Docs |
|---|---|---|
| `extensions/readiness-marker-event-emitter-s3` | `io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3.S3ReadinessMarkerEventEmitterProvider` | [S3](readiness-marker-s3.md) |

If no provider class is set, the default no-op handler from `extensions/readiness-marker-event-emitter-api` is used.
