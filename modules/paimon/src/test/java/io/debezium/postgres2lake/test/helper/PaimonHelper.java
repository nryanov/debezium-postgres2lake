package io.debezium.postgres2lake.test.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.CloseableIterator;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonHelper {
    public record TableRowReader(List<DataField> fields, CloseableIterator<InternalRow> iterator) {}

    private final Catalog catalog;

    public PaimonHelper(String warehouse, PostgresHelper postgresHelper, MinioHelper minioHelper) {
        var config = new Configuration();
        config.set("fs.s3a.access.key", minioHelper.getAccessKey());
        config.set("fs.s3a.secret.key", minioHelper.getSecretAccessKey());
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", minioHelper.endpoint());

        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", warehouse);
        options.set("jdbc-url", postgresHelper.jdbcUrl());
        options.set("jdbc-user", postgresHelper.getUsername());
        options.set("jdbc-password", postgresHelper.getPassword());
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        var catalogContext = CatalogContext.create(options, config);
        catalog = CatalogFactory.createCatalog(catalogContext);
    }

    public TableRowReader readTable(String namespace, String table) {
        try {
            var tableIdentifier = Identifier.create(namespace, table);
            var paimonTable = catalog.getTable(tableIdentifier);

            var tableReaderBuilder = paimonTable.newReadBuilder().newRead();
            var iterator = tableReaderBuilder.createReader(paimonTable.newReadBuilder().newScan().plan().splits())
                    .toCloseableIterator();

            return new TableRowReader(paimonTable.rowType().getFields(), iterator);
        } catch (Catalog.TableNotExistException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Maps a Paimon {@link InternalRow} to a column-name map, resolving null fields via {@link InternalRow#isNullAt(int)}.
     */
    public static Map<String, Object> readRowAsMap(List<DataField> fields, InternalRow row) {
        var values = new HashMap<String, Object>();
        for (var field : fields) {
            var id = field.id();
            if (row.isNullAt(id)) {
                values.put(field.name(), null);
            } else {
                values.put(field.name(), readValue(field.type(), id, row));
            }
        }
        return values;
    }

    private static Object readValue(DataType type, int fieldId, DataGetters row) {
        return switch (type) {
            case org.apache.paimon.types.BooleanType v -> row.getBoolean(fieldId);
            case org.apache.paimon.types.SmallIntType v -> row.getInt(fieldId);
            case org.apache.paimon.types.TinyIntType v -> row.getInt(fieldId);
            case org.apache.paimon.types.IntType v -> row.getInt(fieldId);
            case org.apache.paimon.types.BigIntType v -> row.getLong(fieldId);
            case org.apache.paimon.types.FloatType v -> row.getFloat(fieldId);
            case org.apache.paimon.types.DoubleType v -> row.getDouble(fieldId);
            case org.apache.paimon.types.TimestampType v -> row.getTimestamp(fieldId, 6).toLocalDateTime().atOffset(ZoneOffset.UTC);
            case org.apache.paimon.types.LocalZonedTimestampType v -> row.getTimestamp(fieldId, 6).toLocalDateTime();
            case org.apache.paimon.types.TimeType v -> LocalTime.ofNanoOfDay(row.getInt(fieldId) * 1_000_000L);
            case org.apache.paimon.types.DecimalType v -> row.getDecimal(fieldId, v.getPrecision(), v.getScale()).toBigDecimal();
            case org.apache.paimon.types.DateType v -> LocalDate.ofEpochDay(row.getInt(fieldId));
            case org.apache.paimon.types.VarCharType v -> TypeUtils.readUuidOrString(row.getString(fieldId).toBytes());
            case org.apache.paimon.types.BinaryType v -> TypeUtils.readUuidOrBytes(row.getBinary(fieldId));
            case org.apache.paimon.types.CharType v -> row.getString(fieldId);
            case org.apache.paimon.types.ArrayType v -> {
                var array = row.getArray(fieldId);
                var mappedArray = new ArrayList<>();

                for (var i = 0; i < array.size(); i++) {
                    mappedArray.add(readValue(v.getElementType(), i, array));
                }

                yield mappedArray;
            }
            case org.apache.paimon.types.MapType v -> {
                var map = row.getMap(fieldId);
                var mappedValues = new HashMap<>();

                var keys = map.keyArray();
                var values = map.valueArray();

                for (var i = 0; i < map.size(); i++) {
                    mappedValues.put(readValue(v.getKeyType(), i, keys), readValue(v.getValueType(), i, values));
                }

                yield mappedValues;
            }
            case org.apache.paimon.types.RowType v -> readRowAsMap(v.getFields(), row.getRow(fieldId, v.getFieldCount()));
            default -> throw new IllegalArgumentException("Unsupported paimon type: " + type);
        };
    }
}
