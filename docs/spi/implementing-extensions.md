# Implementing SPI extensions

This guide is for teams that want to create custom SPI modules beyond the reference implementations in this repository.

## SPI model

Each SPI family follows the same pattern:

1. Implement a `*Handler` interface for runtime behavior
2. Implement a `*Provider` interface that creates the handler
3. Register the provider with Java `ServiceLoader`
4. Configure the provider class name through `debezium.extensions.<extension>.name`
5. Pass implementation settings through `debezium.extensions.<extension>.properties.*`

## Available extension families

- Data catalog API (`DataCatalogProvider` / `DataCatalogHandler`)
- Commit event emitter API (`CommitEventEmitterProvider` / `CommitEventEmitterHandler`)
- Readiness marker emitter API (`ReadinessMarkerEventEmitterProvider` / `ReadinessMarkerEventEmitterHandler`)

See [SPI extensions overview](overview.md) and API pages for each extension family.

## Step-by-step implementation

### 1) Implement handler and provider

Implement your target API interfaces from the corresponding `*-api` module.

Your handler should:

- Initialize from a property map (`initialize(Map<String, String>)`)
- Perform the extension-specific action (`emit(...)` or `createOrUpdateTable(...)`)
- Release resources in `close()`

Your provider should create and return the handler instance.

### 2) Register provider via `META-INF/services`

Create a service registration file under:

`src/main/resources/META-INF/services/<ProviderFQCN>`

The file content must be your implementation provider class name (fully qualified).

This is required so `ServiceLoader` can discover your provider.

### 3) Configure extension in application properties

Use the extension key pattern:

- `debezium.extensions.<extension>.name=<your.provider.FQCN>`
- `debezium.extensions.<extension>.properties.<key>=<value>`

If `name` is omitted, the default no-op handler is used.

### 4) Add extension

Build package with your SPI implementation and add it to the project classpath.  

## Design recommendations

- Keep handlers idempotent where possible
- On fail, don't throw error, but log it. Otherwise, main thread of processing events also will fail which is not desired

## Related docs

- [SPI extensions overview](overview.md)
- [Data Catalog API](data-catalog-api.md)
- [Commit Event API](commit-event-api.md)
- [Readiness Marker API](readiness-marker-api.md)
