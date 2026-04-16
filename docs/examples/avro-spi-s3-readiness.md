# Example: Avro + external SPI JAR (S3 readiness marker)

Runs the Avro output pipeline with PostgreSQL CDC, Schema Registry, MinIO, and the Avro app image while loading readiness-marker SPI implementation from a separate JAR mounted on the runtime classpath.
The AVRO image is built with Quarkus `legacy-jar` packaging so SPI JARs can be added at startup classpath time.
The container enables SPI classpath loading via `SPI_EXT_DIR=/work/spi` and `APP_MAIN_CLASS=io.debezium.postgres2lake.avro.bootstrap.Application`.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/avro-spi-s3-readiness` |
| Compose file | `examples/avro-spi-s3-readiness/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-avro:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/avro-spi-s3-readiness/config/application.properties` |
| SPI extension JAR source | `extensions/readiness-marker-event-emitter-s3` |

## Build SPI JAR and place it on classpath

From repository root:

```bash
./gradlew :extensions:readiness-marker-event-emitter-s3:jar
mkdir -p examples/avro-spi-s3-readiness/extensions-jars
cp extensions/readiness-marker-event-emitter-s3/build/libs/*.jar examples/avro-spi-s3-readiness/extensions-jars/
```

## Run

```bash
cd examples/avro-spi-s3-readiness
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

## Verify readiness markers and AVRO output

```bash
docker compose logs app | rg "readiness|S3ReadinessMarkerEventEmitterProvider"
docker compose exec mc mc ls --recursive local/warehouse/readiness-markers
docker compose exec mc mc ls --recursive local/warehouse/postgres/public/demo_orders
```
