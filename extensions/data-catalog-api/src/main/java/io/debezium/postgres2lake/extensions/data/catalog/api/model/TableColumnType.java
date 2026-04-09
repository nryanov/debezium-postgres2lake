package io.debezium.postgres2lake.extensions.data.catalog.api.model;

import java.util.List;


public sealed interface TableColumnType permits TableColumnType.PrimitiveColumnType, TableColumnType.ComplexColumnType {
    TableColumnConstraint constraint();

    sealed interface PrimitiveColumnType extends TableColumnType permits Boolean, Bytes, Date, Decimal, Double, Enum, Fixed, Float, Int, Long, Text, Time, Timestamp, TimestampTz, Uuid {}

    sealed interface ComplexColumnType extends TableColumnType permits Array, Map, Record {}

    record Int(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Text(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Enum(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Boolean(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Long(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Float(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Double(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Fixed(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Bytes(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Decimal(TableColumnConstraint constraint, int scale, int precision) implements PrimitiveColumnType {}

    record Uuid(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Time(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Timestamp(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record TimestampTz(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Date(TableColumnConstraint constraint) implements PrimitiveColumnType {}

    record Map(TableColumnConstraint constraint, PrimitiveColumnType key, TableColumnType value) implements ComplexColumnType {}

    record Array(TableColumnConstraint constraint, TableColumnType element) implements ComplexColumnType {}

    record Record(TableColumnConstraint constraint, List<TableField> nestedFields) implements ComplexColumnType {}

    enum TableColumnConstraint {
        OPTIONAL, REQUIRED, PRIMARY_KEY
    }
}
