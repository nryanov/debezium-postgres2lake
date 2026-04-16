# AVRO example with external SPI JAR (S3 readiness marker)

This example runs the AVRO output pipeline and loads the readiness-marker S3 implementation from a separate extension JAR mounted on the runtime classpath.
The images use Quarkus `legacy-jar` packaging so external SPI JARs can be appended to JVM classpath at startup.
The app container enables SPI classpath mode by setting `SPI_EXT_DIR=/work/spi` and `APP_MAIN_CLASS=io.debezium.postgres2lake.avro.bootstrap.Application`.

## Prerequisites

From repository root:

```bash
./scripts/build-container-images.sh
docker build -t local/jupyter-spark:0.1.0 -f examples/common/jupyter/Dockerfile examples/common/jupyter
```

Build the SPI extension JAR:

```bash
./gradlew :extensions:readiness-marker-event-emitter-s3:jar
```

Copy the produced JAR into this example's classpath directory:

```bash
mkdir -p examples/avro-spi-s3-readiness/extensions-jars
cp extensions/readiness-marker-event-emitter-s3/build/libs/*.jar examples/avro-spi-s3-readiness/extensions-jars/
```

At runtime, `docker-compose.yml` mounts this directory into the container at `/work/spi`,
and the shared container entrypoint prepends `/work/spi/*` to the classpath
before starting `io.debezium.postgres2lake.avro.bootstrap.Application`.

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

Or you can paste script via Jupyter WEB-UI.