package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkDeserializerUtils.*;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

public interface NoahArkRedisArrayDataDeserializer extends Serializable {

    void serializer(ArrayData data, int arrayIndex, ByteBuffer out);

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
            case DISTINCT_TYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                deserializer = (data, arrayIndex, out) -> {
                    ArrayData value = data.getArray(arrayIndex);
                    
                };
                break;
            case MULTISET:
            case MAP:
                // TODO: 2022/6/15  
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(elementType);
                // TODO: 2022/6/15
                break;
            case RAW:
                // TODO: 2022/6/15
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
