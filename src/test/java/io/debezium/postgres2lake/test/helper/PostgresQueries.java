package io.debezium.postgres2lake.test.helper;

public abstract class PostgresQueries {
    public static String createTableWithAllPrimitiveTypes(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s
                (
                    primary_key                            BIGINT PRIMARY KEY,
                    required_column_smallint               SMALLINT                      NOT NULL,
                    required_column_integer                INTEGER                       NOT NULL,
                    required_column_bigint                 BIGINT                        NOT NULL,
                    required_column_decimal                DECIMAL                       NOT NULL,
                    required_column_numeric                NUMERIC(36, 10)               NOT NULL,
                    required_column_real                   REAL                          NOT NULL,
                    required_column_double_precision       DOUBLE PRECISION              NOT NULL,
                    required_column_char                   CHAR(1)                       NOT NULL,
                    required_column_varchar                VARCHAR(255)                  NOT NULL,
                    required_column_text                   TEXT                          NOT NULL,
                    required_column_timestamp              TIMESTAMP WITHOUT TIME ZONE   NOT NULL,
                    required_column_timestamp_tz           TIMESTAMP WITH TIME ZONE      NOT NULL,
                    required_column_date                   DATE                          NOT NULL,
                    required_column_time                   TIME WITHOUT TIME ZONE        NOT NULL,
                    required_column_boolean                BOOLEAN                       NOT NULL,
                    required_column_uuid                   UUID                          NOT NULL,
                
                    optional_column_smallint               SMALLINT,
                    optional_column_integer                INTEGER,
                    optional_column_bigint                 BIGINT,
                    optional_column_decimal                DECIMAL,
                    optional_column_numeric                NUMERIC(36, 10),
                    optional_column_real                   REAL,
                    optional_column_double_precision       DOUBLE PRECISION,
                    optional_column_char                   CHAR(1),
                    optional_column_varchar                VARCHAR(255),
                    optional_column_text                   TEXT,
                    optional_column_timestamp              TIMESTAMP WITHOUT TIME ZONE,
                    optional_column_timestamp_tz           TIMESTAMP WITH TIME ZONE,
                    optional_column_date                   DATE,
                    optional_column_time                   TIME WITHOUT TIME ZONE,
                    optional_column_boolean                BOOLEAN,
                    optional_column_uuid                   UUID,
                
                    required_column_array_smallint         SMALLINT[]                    NOT NULL,
                    required_column_array_integer          INTEGER[]                     NOT NULL,
                    required_column_array_bigint           BIGINT[]                      NOT NULL,
                    required_column_array_decimal          DECIMAL[]                     NOT NULL,
                    required_column_array_numeric          NUMERIC(36, 10)[]             NOT NULL,
                    required_column_array_real             REAL[]                        NOT NULL,
                    required_column_array_double_precision DOUBLE PRECISION[]            NOT NULL,
                    required_column_array_char             CHAR(1)[]                     NOT NULL,
                    required_column_array_varchar          VARCHAR(255)[]                NOT NULL,
                    required_column_array_text             TEXT[]                        NOT NULL,
                    required_column_array_timestamp        TIMESTAMP WITHOUT TIME ZONE[] NOT NULL,
                    required_column_array_timestamp_tz     TIMESTAMP WITH TIME ZONE[]    NOT NULL,
                    required_column_array_date             DATE[]                        NOT NULL,
                    required_column_array_time             TIME WITHOUT TIME ZONE[]      NOT NULL,
                    required_column_array_boolean          BOOLEAN[]                     NOT NULL,
                    required_column_array_uuid             UUID[]                        NOT NULL,
                
                    optional_column_array_smallint         SMALLINT[],
                    optional_column_array_integer          INTEGER[],
                    optional_column_array_bigint           BIGINT[],
                    optional_column_array_decimal          DECIMAL[],
                    optional_column_array_numeric          NUMERIC(36, 10)[],
                    optional_column_array_real             REAL[],
                    optional_column_array_double_precision DOUBLE PRECISION[],
                    optional_column_array_char             CHAR(1)[],
                    optional_column_array_varchar          VARCHAR(255)[],
                    optional_column_array_text             TEXT[],
                    optional_column_array_timestamp        TIMESTAMP WITHOUT TIME ZONE[],
                    optional_column_array_timestamp_tz     TIMESTAMP WITH TIME ZONE[],
                    optional_column_array_date             DATE[],
                    optional_column_array_time             TIME WITHOUT TIME ZONE[],
                    optional_column_array_boolean          BOOLEAN[],
                    optional_column_array_uuid             UUID[]
                )
                """, table);
    }

    public static String insertIntoTableWithAllPrimitiveTypes(String table, long pk) {
        return String.format("""
                INSERT INTO %s (
                    primary_key,
                    required_column_smallint,
                    required_column_integer,
                    required_column_bigint,
                    required_column_decimal,
                    required_column_numeric,
                    required_column_real,
                    required_column_double_precision,
                    required_column_char,
                    required_column_varchar,
                    required_column_text,
                    required_column_timestamp,
                    required_column_timestamp_tz,
                    required_column_date,
                    required_column_time,
                    required_column_boolean,
                    required_column_uuid,
                    required_column_array_smallint,
                    required_column_array_integer,
                    required_column_array_bigint,
                    required_column_array_decimal,
                    required_column_array_numeric,
                    required_column_array_real,
                    required_column_array_double_precision,
                    required_column_array_char,
                    required_column_array_varchar,
                    required_column_array_text,
                    required_column_array_timestamp,
                    required_column_array_timestamp_tz,
                    required_column_array_date,
                    required_column_array_time,
                    required_column_array_boolean,
                    required_column_array_uuid
                ) VALUES (
                    %d,
                    1,
                    2,
                    3,
                    1.5,
                    1.1234567890,
                    1.5::real,
                    1.5::double precision,
                    'a',
                    'varchar',
                    'text',
                    '2020-01-01 12:00:00'::timestamp without time zone,
                    '2020-01-01 12:00:00+00'::timestamptz,
                    '2020-01-01'::date,
                    '12:34:56'::time without time zone,
                    true,
                    '550e8400-e29b-41d4-a716-446655440000'::uuid,
                    ARRAY[1::smallint, 2::smallint],
                    ARRAY[1, 2],
                    ARRAY[1::bigint, 2::bigint],
                    ARRAY[1.0::decimal, 2.0::decimal],
                    ARRAY[1.1234567890::numeric(36,10), 2.1234567890::numeric(36,10)],
                    ARRAY[1.0::real, 2.0::real],
                    ARRAY[1.0::float8, 2.0::float8],
                    ARRAY['a'::char(1), 'b'::char(1)],
                    ARRAY['a'::varchar, 'b'::varchar],
                    ARRAY['a'::text, 'b'::text],
                    ARRAY['2020-01-01 12:00:00'::timestamp without time zone, '2020-01-02 12:00:00'::timestamp without time zone],
                    ARRAY['2020-01-01 12:00:00+00'::timestamptz, '2020-01-02 12:00:00+00'::timestamptz],
                    ARRAY['2020-01-01'::date, '2020-01-02'::date],
                    ARRAY['12:00:00'::time, '13:00:00'::time],
                    ARRAY[true, false],
                    ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid, '650e8400-e29b-41d4-a716-446655440000'::uuid]
                )
                """, table, pk);
    }

    public static String createSmallintTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       SMALLINT    NOT NULL,
                    optional_field       SMALLINT,
                    required_array_field SMALLINT[]  NOT NULL,
                    optional_array_field SMALLINT[]
                )
                """, table);
    }

    public static String insertSmallintRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1::smallint, 2::smallint, ARRAY[1::smallint, 2::smallint], ARRAY[3::smallint, 4::smallint])
                """, table, pk);
    }

    public static String createIntegerTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       INTEGER    NOT NULL,
                    optional_field       INTEGER,
                    required_array_field INTEGER[]  NOT NULL,
                    optional_array_field INTEGER[]
                )
                """, table);
    }

    public static String insertIntegerRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1, 2, ARRAY[1, 2], ARRAY[3, 4])
                """, table, pk);
    }

    public static String createBigintTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       BIGINT    NOT NULL,
                    optional_field       BIGINT,
                    required_array_field BIGINT[]  NOT NULL,
                    optional_array_field BIGINT[]
                )
                """, table);
    }

    public static String insertBigintRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1::bigint, 2::bigint, ARRAY[1::bigint, 2::bigint], ARRAY[3::bigint, 4::bigint])
                """, table, pk);
    }

    public static String createDecimalTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       DECIMAL    NOT NULL,
                    optional_field       DECIMAL,
                    required_array_field DECIMAL[]  NOT NULL,
                    optional_array_field DECIMAL[]
                )
                """, table);
    }

    public static String insertDecimalRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1.5, 2.5, ARRAY[1.0::decimal, 2.0::decimal], ARRAY[3.0::decimal, 4.0::decimal])
                """, table, pk);
    }

    public static String createNumericTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       NUMERIC(36, 10)    NOT NULL,
                    optional_field       NUMERIC(36, 10),
                    required_array_field NUMERIC(36, 10)[]  NOT NULL,
                    optional_array_field NUMERIC(36, 10)[]
                )
                """, table);
    }

    public static String insertNumericRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    1.1234567890::numeric(36,10),
                    2.1234567890::numeric(36,10),
                    ARRAY[1.1234567890::numeric(36,10), 2.1234567890::numeric(36,10)],
                    ARRAY[3.1234567890::numeric(36,10), 4.1234567890::numeric(36,10)])
                """, table, pk);
    }

    public static String createRealTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       REAL    NOT NULL,
                    optional_field       REAL,
                    required_array_field REAL[]  NOT NULL,
                    optional_array_field REAL[]
                )
                """, table);
    }

    public static String insertRealRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1.5::real, 2.5::real, ARRAY[1.0::real, 2.0::real], ARRAY[3.0::real, 4.0::real])
                """, table, pk);
    }

    public static String createDoublePrecisionTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       DOUBLE PRECISION    NOT NULL,
                    optional_field       DOUBLE PRECISION,
                    required_array_field DOUBLE PRECISION[]  NOT NULL,
                    optional_array_field DOUBLE PRECISION[]
                )
                """, table);
    }

    public static String insertDoublePrecisionRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 1.5::float8, 2.5::float8, ARRAY[1.0::float8, 2.0::float8], ARRAY[3.0::float8, 4.0::float8])
                """, table, pk);
    }

    public static String createCharTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       CHAR(1)    NOT NULL,
                    optional_field       CHAR(1),
                    required_array_field CHAR(1)[]  NOT NULL,
                    optional_array_field CHAR(1)[]
                )
                """, table);
    }

    public static String insertCharRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 'a', 'b', ARRAY['a'::char(1), 'b'::char(1)], ARRAY['c'::char(1), 'd'::char(1)])
                """, table, pk);
    }

    public static String createVarcharTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       VARCHAR(255)    NOT NULL,
                    optional_field       VARCHAR(255),
                    required_array_field VARCHAR(255)[]  NOT NULL,
                    optional_array_field VARCHAR(255)[]
                )
                """, table);
    }

    public static String insertVarcharRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 'abc', 'def', ARRAY['abc'::varchar, 'def'::varchar], ARRAY['ghi'::varchar, 'jkl'::varchar])
                """, table, pk);
    }

    public static String createTextTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       TEXT    NOT NULL,
                    optional_field       TEXT,
                    required_array_field TEXT[]  NOT NULL,
                    optional_array_field TEXT[]
                )
                """, table);
    }

    public static String insertTextRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, 'hello', 'world', ARRAY['hello'::text, 'world'::text], ARRAY['foo'::text, 'bar'::text])
                """, table, pk);
    }

    public static String createTimestampTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       TIMESTAMP WITHOUT TIME ZONE    NOT NULL,
                    optional_field       TIMESTAMP WITHOUT TIME ZONE,
                    required_array_field TIMESTAMP WITHOUT TIME ZONE[]  NOT NULL,
                    optional_array_field TIMESTAMP WITHOUT TIME ZONE[]
                )
                """, table);
    }

    public static String insertTimestampRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    '2020-01-01 12:00:00'::timestamp without time zone,
                    '2020-06-15 18:30:00'::timestamp without time zone,
                    ARRAY['2020-01-01 12:00:00'::timestamp without time zone, '2020-01-02 12:00:00'::timestamp without time zone],
                    ARRAY['2020-06-15 18:30:00'::timestamp without time zone, '2020-06-16 18:30:00'::timestamp without time zone])
                """, table, pk);
    }

    public static String createTimestampTzTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       TIMESTAMP WITH TIME ZONE    NOT NULL,
                    optional_field       TIMESTAMP WITH TIME ZONE,
                    required_array_field TIMESTAMP WITH TIME ZONE[]  NOT NULL,
                    optional_array_field TIMESTAMP WITH TIME ZONE[]
                )
                """, table);
    }

    public static String insertTimestampTzRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    '2020-01-01 12:00:00+00'::timestamptz,
                    '2020-06-15 18:30:00+00'::timestamptz,
                    ARRAY['2020-01-01 12:00:00+00'::timestamptz, '2020-01-02 12:00:00+00'::timestamptz],
                    ARRAY['2020-06-15 18:30:00+00'::timestamptz, '2020-06-16 18:30:00+00'::timestamptz])
                """, table, pk);
    }

    public static String createDateTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       DATE    NOT NULL,
                    optional_field       DATE,
                    required_array_field DATE[]  NOT NULL,
                    optional_array_field DATE[]
                )
                """, table);
    }

    public static String insertDateRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    '2020-01-01'::date,
                    '2020-06-15'::date,
                    ARRAY['2020-01-01'::date, '2020-01-02'::date],
                    ARRAY['2020-06-15'::date, '2020-06-16'::date])
                """, table, pk);
    }

    public static String createTimeTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       TIME WITHOUT TIME ZONE    NOT NULL,
                    optional_field       TIME WITHOUT TIME ZONE,
                    required_array_field TIME WITHOUT TIME ZONE[]  NOT NULL,
                    optional_array_field TIME WITHOUT TIME ZONE[]
                )
                """, table);
    }

    public static String insertTimeRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    '12:34:56'::time without time zone,
                    '08:15:30'::time without time zone,
                    ARRAY['12:00:00'::time, '13:00:00'::time],
                    ARRAY['08:00:00'::time, '09:00:00'::time])
                """, table, pk);
    }

    public static String createBooleanTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       BOOLEAN    NOT NULL,
                    optional_field       BOOLEAN,
                    required_array_field BOOLEAN[]  NOT NULL,
                    optional_array_field BOOLEAN[]
                )
                """, table);
    }

    public static String insertBooleanRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d, true, false, ARRAY[true, false], ARRAY[false, true])
                """, table, pk);
    }

    // --- UUID ---

    public static String createUuidTable(String table) {
        return String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    primary_key          BIGINT PRIMARY KEY,
                    required_field       UUID    NOT NULL,
                    optional_field       UUID,
                    required_array_field UUID[]  NOT NULL,
                    optional_array_field UUID[]
                )
                """, table);
    }

    public static String insertUuidRow(String table, long pk) {
        return String.format("""
                INSERT INTO %s (primary_key, required_field, optional_field, required_array_field, optional_array_field)
                VALUES (%d,
                    '550e8400-e29b-41d4-a716-446655440000'::uuid,
                    '650e8400-e29b-41d4-a716-446655440000'::uuid,
                    ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid, '650e8400-e29b-41d4-a716-446655440000'::uuid],
                    ARRAY['750e8400-e29b-41d4-a716-446655440000'::uuid, '850e8400-e29b-41d4-a716-446655440000'::uuid])
                """, table, pk);
    }

    public static String createLogicalSlot(String slotName) {
        return String.format("SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('%s', 'pgoutput')", slotName);
    }

    public static String createPublication(String publication) {
        return String.format("CREATE PUBLICATION %s", publication);
    }

    public static String addTableToPublication(String publication, String table) {
        return String.format("ALTER PUBLICATION %s ADD TABLE %s", publication, table);
    }
}
