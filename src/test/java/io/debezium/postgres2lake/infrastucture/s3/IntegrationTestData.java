package io.debezium.postgres2lake.infrastucture.s3;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/**
 * Inserts one fully-populated row into {@code public.data} for integration tests.
 */
public final class IntegrationTestData {

    private IntegrationTestData() {
    }

    public static void insertSampleRow(Connection conn, long primaryKey) throws SQLException {
        var sql = String.format(Locale.ROOT, """
                INSERT INTO public.data (
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
                """, primaryKey);
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
    }
}
