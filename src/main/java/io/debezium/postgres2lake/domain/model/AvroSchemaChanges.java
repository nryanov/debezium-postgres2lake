package io.debezium.postgres2lake.domain.model;


import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public record AvroSchemaChanges(List<ColumnChange> changes) {
    public sealed interface ColumnChange extends Serializable permits
            AddColumn,
            DeleteColumn,
            MakeOptional,
            WideColumnType {
        ChangeType changeType();
    }

    // parentColumn -- empty list, if no parent column (root table will be used)
    public record AddColumn(List<String> parentColumn, String name, Schema type) implements ColumnChange {
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

        public String[] columnNameParts() {
            if (parentColumn.isEmpty()) {
                return new String[]{name};
            }

            var array = new String[parentColumn.size() + 1];

            for (var i = 0; i < parentColumn.size(); i++) {
                array[i] = parentColumn.get(i);
            }
            array[array.length - 1] = name;

            return array;
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

        public String[] columnNameParts() {
            if (parentColumn.isEmpty()) {
                return new String[]{name};
            }

            var array = new String[parentColumn.size() + 1];

            for (var i = 0; i < parentColumn.size(); i++) {
                array[i] = parentColumn.get(i);
            }
            array[array.length - 1] = name;

            return array;
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

        public String[] columnNameParts() {
            if (parentColumn.isEmpty()) {
                return new String[]{name};
            }

            var array = new String[parentColumn.size() + 1];

            for (var i = 0; i < parentColumn.size(); i++) {
                array[i] = parentColumn.get(i);
            }
            array[array.length - 1] = name;

            return array;
        }
    }

    public record WideColumnType(List<String> parentColumn, String name, Schema type) implements ColumnChange {
        @Override
        public ChangeType changeType() {
            return ChangeType.WIDE;
        }

        public String fullColumnName() {
            var parentColumnNames = new ArrayList<>(parentColumn);
            parentColumnNames.add(name);

            return String.join(".", parentColumnNames);
        }

        public String[] columnNameParts() {
            if (parentColumn.isEmpty()) {
                return new String[]{name};
            }

            var array = new String[parentColumn.size() + 1];

            for (var i = 0; i < parentColumn.size(); i++) {
                array[i] = parentColumn.get(i);
            }
            array[array.length - 1] = name;

            return array;
        }
    }

    public enum ChangeType {
        ADD, DELETE, MAKE_OPTIONAL, WIDE
    }
}
