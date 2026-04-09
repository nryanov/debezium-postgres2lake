package io.debezium.postgres2lake.extensions.data.catalog.api.model;

import java.util.List;

public record TableSchema(List<TableField> fields) {}
