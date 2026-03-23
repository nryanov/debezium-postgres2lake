package io.debezium.postgres2lake.test;

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
