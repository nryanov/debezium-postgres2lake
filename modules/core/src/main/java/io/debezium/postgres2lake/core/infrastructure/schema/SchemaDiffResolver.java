package io.debezium.postgres2lake.core.infrastructure.schema;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.IncompatibleChangeToRequiredException;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.IncompatibleTypePromotion;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.NonPrimitiveFieldPromotionException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class SchemaDiffResolver {
    public AvroSchemaChanges resolveDiff(Schema current, Schema next) {
        var changes = new ArrayList<AvroSchemaChanges.ColumnChange>();
        resolveSchemaFieldsDiff(new ArrayList<>(), current, next, changes);
        return new AvroSchemaChanges(changes);
    }

    private void resolveSchemaFieldsDiff(
            List<String> parentColumns,
            org.apache.avro.Schema currentSchema,
            org.apache.avro.Schema newSchema,
            List<AvroSchemaChanges.ColumnChange> changes) {
        var currentFields = currentSchema.getFields();
        var newFields = newSchema.getFields();

        // check for deleted and changed fields
        for (var existingField : currentFields) {
            var newField = newSchema.getField(existingField.name());
            if (newField == null) { // if field was deleted
                changes.add(new AvroSchemaChanges.DeleteColumn(parentColumns, existingField.name()));
            } else if (!areFieldsEqual(existingField, newField)) { // if existing field has updates
                var existingFieldSchema = existingField.schema();
                var newFieldSchema = newField.schema();

                // check if field should become nullable
                if (!existingFieldSchema.isNullable() && newFieldSchema.isNullable()) {
                    changes.add(new AvroSchemaChanges.MakeOptional(parentColumns, existingField.name()));
                }

                // illegal attempt to make field required
                if (existingFieldSchema.isNullable() && !newFieldSchema.isNullable()) {
                    throw new IncompatibleChangeToRequiredException(existingField);
                }

                // check if it is a valid type promotion
                if (isTypePromotion(existingFieldSchema, newFieldSchema)) {
                    changes.add(resolveFieldTypePromotion(parentColumns, existingField, newField));
                }

                // if this is a nested-field then check also for deep updates
                var maybeExistingNestedRecord = extractRecordSchema(existingField.schema());
                var maybeNewNestedRecord = extractRecordSchema(newField.schema());
                if (maybeExistingNestedRecord != null && maybeNewNestedRecord != null) {
                    var existingFieldParents = new ArrayList<>(parentColumns);
                    existingFieldParents.add(existingField.name());
                    resolveSchemaFieldsDiff(
                            existingFieldParents, maybeExistingNestedRecord, maybeNewNestedRecord, changes);
                }
            }
        }

        // check for added fields
        for (var newField : newFields) {
            var existingField = currentSchema.getField(newField.name());
            if (existingField == null) {
                changes.add(new AvroSchemaChanges.AddColumn(parentColumns, newField.name(), newField.schema()));
            }
        }
    }

    private boolean isTypePromotion(org.apache.avro.Schema existingField, org.apache.avro.Schema newField) {
        // both fields are non-primitive
        if (!isPrimitive(existingField) && !isPrimitive(newField)) {
            return false;
        }

        // if one field is primitive and another -- non-primitive
        var isIncorrectPromotion = isPrimitive(existingField) ^ isPrimitive(newField);

        // check if both fields are primitive
        if (isIncorrectPromotion) {
            throw new NonPrimitiveFieldPromotionException(existingField, newField);
        }

        var existingFieldType = extractType(existingField);
        var newFieldType = extractType(newField);

        return !existingFieldType.equals(newFieldType);
    }

    private AvroSchemaChanges.ColumnChange resolveFieldTypePromotion(
            List<String> parentColumns,
            org.apache.avro.Schema.Field existingField,
            org.apache.avro.Schema.Field newField) {
        var existingType = extractType(existingField.schema());
        var newType = extractType(newField.schema());

        if (existingType.getType() == org.apache.avro.Schema.Type.INT
                && newType.getType() == org.apache.avro.Schema.Type.LONG) {
            return new AvroSchemaChanges.WideColumnType(
                    parentColumns, existingField.name(), newField.schema());
        }

        if (existingType.getType() == org.apache.avro.Schema.Type.FLOAT
                && newType.getType() == org.apache.avro.Schema.Type.DOUBLE) {
            return new AvroSchemaChanges.WideColumnType(
                    parentColumns, existingField.name(), newField.schema());
        }

        if (existingType.getType() == org.apache.avro.Schema.Type.BYTES
                && newType.getType() == org.apache.avro.Schema.Type.BYTES
                && existingType.getLogicalType() instanceof LogicalTypes.Decimal existingDecimal
                && newType.getLogicalType() instanceof LogicalTypes.Decimal newDecimal
                && existingDecimal.getPrecision() < newDecimal.getPrecision()) {
            return new AvroSchemaChanges.WideColumnType(
                    parentColumns,
                    existingField.name(),
                    newField.schema());
        }

        throw new IncompatibleTypePromotion(existingField.schema(), newField.schema());
    }

    private boolean areFieldsEqual(
            org.apache.avro.Schema.Field currentField, org.apache.avro.Schema.Field newField) {
        return currentField.schema().equals(newField.schema())
                && currentField.schema().isNullable() == newField.schema().isNullable()
                && currentField.schema().getType().equals(newField.schema().getType());
    }

    private org.apache.avro.Schema extractType(org.apache.avro.Schema schema) {
        if (schema.isUnion()) {
            return schema.getTypes().get(1);
        }

        return schema;
    }

    private boolean isPrimitive(org.apache.avro.Schema schema) {
        return switch (schema.getType()) {
            case MAP, ARRAY, RECORD -> false;
            case UNION -> {
                var types = schema.getTypes();

                yield types.size() == 2 && isPrimitive(types.get(1));
            }
            default -> true;
        };
    }

    private org.apache.avro.Schema extractRecordSchema(org.apache.avro.Schema schema) {
        var maybeRecordSchema = schema;

        if (schema.isUnion()) {
            maybeRecordSchema = schema.getTypes().get(1);
        }

        if (maybeRecordSchema.getType() == org.apache.avro.Schema.Type.RECORD) {
            return maybeRecordSchema;
        }

        return null;
    }
}
