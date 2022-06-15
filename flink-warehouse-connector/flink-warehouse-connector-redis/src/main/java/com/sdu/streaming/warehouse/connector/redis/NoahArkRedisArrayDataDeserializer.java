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

public interface NoahArkRedisArrayDataDeserializer extends Serializable {

    void serializer(ArrayData data, int arrayIndex, DataOutput out) throws IOException;

    static NoahArkRedisArrayDataDeserializer createRedisArrayDataDeserializer(LogicalType elementType) {
        final NoahArkRedisArrayDataDeserializer deserializer;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                deserializer = (data, arrayIndex, out) -> {
                    StringData value = data.getString(arrayIndex);
                    serializeStringData(value, out);
                };
                break;
            case BOOLEAN:
                deserializer = (data, arrayIndex, out) -> {
                    boolean value = data.getBoolean(arrayIndex);
                    serializeBooleanData(value, out);
                };
                break;
            case BINARY:
            case VARBINARY:
                deserializer = (data, arrayIndex, out) -> {
                    byte[] values = data.getBinary(arrayIndex);
                    serializeBinaryData(values, out);
                };
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                deserializer = (data, arrayIndex, out) -> {
                    DecimalData value = data.getDecimal(arrayIndex, decimalPrecision, decimalScale);
                    serializeDecimalData(value, out);
                };
                break;
            case TINYINT:
                deserializer = (data, arrayIndex, out) -> {
                    byte value = data.getByte(arrayIndex);
                    serializeByteData(value, out);
                };
                break;
            case SMALLINT:
                deserializer = (data, arrayIndex, out) -> {
                    short value = data.getShort(arrayIndex);
                    serializeShortData(value, out);
                };
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                deserializer = (data, arrayIndex, out) -> {
                    int value = data.getInt(arrayIndex);
                    serializeIntData(value, out);
                };
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                deserializer = (data, arrayIndex, out) -> {
                    long value = data.getLong(arrayIndex);
                    serializeLongData(value, out);
                };
                break;
            case FLOAT:
                deserializer = (data, arrayIndex, out) -> {
                    float value = data.getFloat(arrayIndex);
                    serializeFloatData(value, out);
                };
                break;
            case DOUBLE:
                deserializer = (data, arrayIndex, out) -> {
                    double value = data.getDouble(arrayIndex);
                    serializeDoubleData(value, out);
                };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                deserializer = (data, arrayIndex, out) -> {
                    TimestampData value = data.getTimestamp(arrayIndex, timestampPrecision);
                    serializeTimestampData(value, out);
                };
                break;
            case RAW:
            case DISTINCT_TYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                deserializer = (data, arrayIndex, out) -> {
                    ArrayData value = data.getArray(arrayIndex);
                    ArrayType arrayType = (ArrayType) elementType;
                    serializeArrayData(value, arrayType.getElementType(), out);
                };
                break;
            case MULTISET:
            case MAP:
                deserializer = (data, arrayIndex, out) -> {
                    MapData value = data.getMap(arrayIndex);
                    MapType type = (MapType) elementType;
                    serializeMapData(value, type.getKeyType(), type.getValueType(), out);
                };
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(elementType);
                deserializer = (data, arrayIndex, out) -> {
                    RowData value = data.getRow(arrayIndex, rowFieldCount);
                    RowType rowType = (RowType) elementType;
                    serializeRowData(value, rowType, out);
                };
                break;

            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }

        if (!elementType.isNullable()) {
            return deserializer;
        }

        return (data, arrayIndex, out) -> {
            if (data.isNullAt(arrayIndex)) {
                return;
            }
            deserializer.serializer(data, arrayIndex, out);
        };
    }

}
