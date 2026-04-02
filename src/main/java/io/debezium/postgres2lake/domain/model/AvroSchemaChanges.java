package io.debezium.postgres2lake.domain.model;


import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public record AvroSchemaChanges(List<ColumnChange> changes) {
    public sealed interface ColumnChange extends Serializable permits
            AddColumn,
            DeleteColumn,
            MakeOptional,
            WideColumnType {
        ChangeType changeType();
    }

    // parentColumn -- empty list, if no parent column (root table will be used)
    public record AddColumn(List<String> parentColumn, String name, Type type) implements ColumnChange {
        @Override
        public ChangeType changeType() {
            return ChangeType.ADD;
        }

        public String parentColumnName() {
            if (parentColumn.isEmpty()) {
                return null;
            }

            return String.join(".", parentColumn());
        }
    }

    public record DeleteColumn(List<String> parentColumn, String name) implements ColumnChange {
        @Override
        public ChangeType changeType() {
            return ChangeType.DELETE;
        }

        public String fullColumnName() {
            var parentColumnNames = new ArrayList<>(parentColumn);
            parentColumnNames.add(name);

            return String.join(".", parentColumnNames);
        }
    }

    public record MakeOptional(List<String> parentColumn, String name) implements ColumnChange {
        @Override
        public ChangeType changeType() {
            return ChangeType.MAKE_OPTIONAL;
        }

        public String fullColumnName() {
            var parentColumnNames = new ArrayList<>(parentColumn);
            parentColumnNames.add(name);

            return String.join(".", parentColumnNames);
        }
    }

    public record WideColumnType(List<String> parentColumn, String name, Type type) implements ColumnChange {
        @Override
        public ChangeType changeType() {
            return ChangeType.WIDE;
        }

        public String fullColumnName() {
            var parentColumnNames = new ArrayList<>(parentColumn);
            parentColumnNames.add(name);

            return String.join(".", parentColumnNames);
        }
    }

    public record Type(Schema.Type type, Optional<LogicalType> logicalType) {
    }

    public enum ChangeType {
        ADD, DELETE, MAKE_OPTIONAL, WIDE
    }
}
