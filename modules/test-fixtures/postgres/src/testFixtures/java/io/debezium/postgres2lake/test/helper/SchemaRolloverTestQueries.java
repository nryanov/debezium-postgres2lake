package io.debezium.postgres2lake.test.helper;

public abstract class SchemaRolloverTestQueries {

    public static String createMinimalSchemaRolloverTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key BIGINT PRIMARY KEY,
                    v TEXT NOT NULL
                )
                """, table);
    }

    public static String schemaRolloverTransaction(String table) {
        return String.format(
                """
                        BEGIN;
                        INSERT INTO %s (primary_key, v) VALUES (1, 'first');
                        ALTER TABLE %s ADD COLUMN new_col TEXT;
                        INSERT INTO %s (primary_key, v, new_col) VALUES (2, 'second', 'extra');
                        COMMIT;
                        """,
                table,
                table,
                table);
    }

    public static String createPartitionRolloverTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key BIGINT PRIMARY KEY,
                    lake_part TEXT NOT NULL,
                    v TEXT NOT NULL
                )
                """, table);
    }

    public static String partitionRolloverTransaction(String table) {
        return String.format(
                """
                        BEGIN;
                        INSERT INTO %s (primary_key, lake_part, v) VALUES (1, 'a', 'first');
                        INSERT INTO %s (primary_key, lake_part, v) VALUES (2, 'b', 'second');
                        COMMIT;
                        """,
                table,
                table);
    }
}
