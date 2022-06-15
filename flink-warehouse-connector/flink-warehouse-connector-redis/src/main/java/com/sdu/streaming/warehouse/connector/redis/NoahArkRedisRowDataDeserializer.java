package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkDeserializerUtils.*;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

public interface NoahArkRedisRowDataDeserializer extends Serializable {

    void serializer(RowData data, int fieldPos, DataOutput out) throws IOException;


    static NoahArkRedisRowDataDeserializer createRedisRowDataDeserializer(LogicalType fieldType) {
        NoahArkRedisRowDataDeserializer deserializer;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                deserializer = (data, fieldPos, out) -> {
                    StringData value = data.getString(fieldPos);
                    serializeStringData(value, out);
                };
                break;
            case BOOLEAN:
                deserializer = (data, fieldPos, out) -> {
                    boolean value = data.getBoolean(fieldPos);
                    serializeBooleanData(value, out);
                };
                break;
            case BINARY:
            case VARBINARY:
                deserializer = (data, fieldPos, out) -> {
                    byte[] values = data.getBinary(fieldPos);
                    serializeBinaryData(values, out);
                };
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                deserializer = (data, fieldPos, out) -> {
                    DecimalData value = data.getDecimal(fieldPos, decimalPrecision, decimalScale);
                    serializeDecimalData(value, out);
                };
                break;
            case TINYINT:
                deserializer = (data, fieldPos, out) -> {
                    byte value = data.getByte(fieldPos);
                    serializeByteData(value, out);
                };
                break;
            case SMALLINT:
                deserializer = (data, fieldPos, out) -> {
                    short value = data.getShort(fieldPos);
                    serializeShortData(value, out);
                };
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                deserializer = (data, fieldPos, out) -> {
                    int value = data.getInt(fieldPos);
                    serializeIntData(value, out);
                };
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                deserializer = (data, fieldPos, out) -> {
                    long value = data.getLong(fieldPos);
                    serializeLongData(value, out);
                };
                break;
            case FLOAT:
                deserializer = (data, fieldPos, out) -> {
                    float value = data.getFloat(fieldPos);
                    serializeFloatData(value, out);
                };
                break;
            case DOUBLE:
                deserializer = (data, fieldPos, out) -> {
                    double value = data.getDouble(fieldPos);
                    serializeDoubleData(value, out);
                };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                deserializer = (data, fieldPos, out) -> {
                    TimestampData value = data.getTimestamp(fieldPos, timestampPrecision);
                    serializeTimestampData(value, out);
                };
                break;
            case RAW:
            case DISTINCT_TYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                deserializer = (data, fieldPos, out) -> {
                    ArrayData value = data.getArray(fieldPos);
                    ArrayType arrayType = (ArrayType) fieldType;
                    serializeArrayData(value, arrayType.getElementType(), out);
                };
                break;
            case MULTISET:
            case MAP:
                deserializer = (data, fieldPos, out) -> {
                    MapData value = data.getMap(fieldPos);
                    MapType type = (MapType) fieldType;
                    serializeMapData(value, type.getKeyType(), type.getValueType(), out);
                };
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                deserializer = (data, fieldPos, out) -> {
                    RowData value = data.getRow(fieldPos, rowFieldCount);
                    RowType rowType = (RowType) fieldType;
                    serializeRowData(value, rowType, out);
                };
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }

        if (!fieldType.isNullable()) {
            return deserializer;
        }
        return (data, fieldPos, out) -> {
            if (data.isNullAt(fieldPos)) {
                return;
            }
            deserializer.serializer(data, fieldPos, out);
        };
    }


}
