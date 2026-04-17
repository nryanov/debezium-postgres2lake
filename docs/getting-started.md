# Getting Started

## Prerequisites

- Java 21
- Docker + Docker Compose v2.20+
- Python (only if you want to run helper scripts outside the `jupyter` container)

For environment details beyond quick-start setup, see [Requirements](requirements.md).
For shared CDC engine and payload settings, see [Debezium configuration](debezium-configuration.md).

## Build and test

Build all modules:

```bash
./gradlew build
```

Run core tests only:

```bash
./gradlew :modules:core:test
```

Run one format module build:

```bash
./gradlew :modules:iceberg:build
```

## Build application images

From repository root:

```bash
./scripts/build-container-images.sh
```

The script builds local images used by examples.

## Run first example

Start one example stack at a time:

```bash
cd examples/iceberg-jdbc
docker compose up -d
```

Then generate source traffic:

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

## Next steps

- Review all available examples in `examples/`
- See [Examples](examples.md) for short format-specific runs
- Explore [Formats](formats.md) for module details
- Review [Features](features.md) for supported CDC behavior
- Review [Schema Evolution](schema-evolution.md) for supported schema change handling
