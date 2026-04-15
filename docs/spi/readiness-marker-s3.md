# SPI: Readiness Marker Emitter (S3)

S3 readiness marker emitter writes a JSON marker object to S3-compatible storage after commit cycles.

Provider class:
`io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3.S3ReadinessMarkerEventEmitterProvider`

API module: `extensions/readiness-marker-event-emitter-api`

## Configuration example

```properties
debezium.extensions.readiness-marker-event-emitter.name=io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3.S3ReadinessMarkerEventEmitterProvider

debezium.extensions.readiness-marker-event-emitter.properties.bucket=warehouse
debezium.extensions.readiness-marker-event-emitter.properties.marker-key-prefix=readiness-markers
debezium.extensions.readiness-marker-event-emitter.properties.region=us-east-1
debezium.extensions.readiness-marker-event-emitter.properties.endpoint=http://minio:9000
debezium.extensions.readiness-marker-event-emitter.properties.access-key-id=admin
debezium.extensions.readiness-marker-event-emitter.properties.secret-access-key=password
debezium.extensions.readiness-marker-event-emitter.properties.path-style-access=true
```

## Available configs

| Property | Required | Description | Values / examples |
|---|---|---|---|
| `debezium.extensions.readiness-marker-event-emitter.name` | Yes | SPI provider class name | `...S3ReadinessMarkerEventEmitterProvider` |
| `debezium.extensions.readiness-marker-event-emitter.properties.bucket` | Yes | Target S3 bucket | `warehouse` |
| `debezium.extensions.readiness-marker-event-emitter.properties.marker-key` | No | Explicit marker object key (overrides timestamp naming) | `markers/latest.json` |
| `debezium.extensions.readiness-marker-event-emitter.properties.marker-key-prefix` | No | Marker prefix when `marker-key` is not set | default `readiness-markers` |
| `debezium.extensions.readiness-marker-event-emitter.properties.region` or `.aws.region` | No | AWS region | default `us-east-1` |
| `debezium.extensions.readiness-marker-event-emitter.properties.endpoint` or `.aws.endpoint` | No | Custom S3 endpoint | `http://minio:9000` |
| `debezium.extensions.readiness-marker-event-emitter.properties.access-key-id` | No | Access key ID | `admin` |
| `debezium.extensions.readiness-marker-event-emitter.properties.secret-access-key` | No | Secret access key (required when access key is set) | `password` |
| `debezium.extensions.readiness-marker-event-emitter.properties.session-token` | No | Session token for temporary credentials | token value |
| `debezium.extensions.readiness-marker-event-emitter.properties.path-style-access` or `.s3.path-style-access` | No | Use path-style S3 addressing | `true`, `false` |
