package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkDeserializerUtils.*;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

public interface NoahArkRedisRowDataDeserializer extends Serializable {

    void serializer(RowData data, ByteBuffer out);


    static NoahArkRedisRowDataDeserializer createRedisDataObjectDeserializer(LogicalType fieldType, int fieldPos) {
        NoahArkRedisRowDataDeserializer deserializer;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                deserializer = (data, out) -> {
                    StringData value = data.getString(fieldPos);
                    serializeStringData(value, out);
                };
                break;
            case BOOLEAN:
                deserializer = (data, out) -> {
                    boolean value = data.getBoolean(fieldPos);
                    serializeBooleanData(value, out);
                };
                break;
            case BINARY:
            case VARBINARY:
                deserializer = (data, out) -> {
                    byte[] values = data.getBinary(fieldPos);
                    serializeBinaryData(values, out);
                };
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                deserializer = (data, out) -> {
                    DecimalData value = data.getDecimal(fieldPos, decimalPrecision, decimalScale);
                    serializeDecimalData(value, out);
                };
                break;
            case TINYINT:
                deserializer = (data, out) -> {
                    byte value = data.getByte(fieldPos);
                    serializeByteData(value, out);
                };
                break;
            case SMALLINT:
                deserializer = (data, out) -> {
                    short value = data.getShort(fieldPos);
                    serializeShortData(value, out);
                };
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                deserializer = (data, out) -> {
                    int value = data.getInt(fieldPos);
                    serializeIntData(value, out);
                };
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                deserializer = (data, out) -> {
                    long value = data.getLong(fieldPos);
                    serializeLongData(value, out);
                };
                break;
            case FLOAT:
                deserializer = (data, out) -> {
                    float value = data.getFloat(fieldPos);
                    serializeFloatData(value, out);
                };
                break;
            case DOUBLE:
                deserializer = (data, out) -> {
                    double value = data.getDouble(fieldPos);
                    serializeDoubleData(value, out);
                };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                deserializer = (data, out) -> {
                    TimestampData value = data.getTimestamp(fieldPos, timestampPrecision);
                    serializeTimestampData(value, out);
                };
                break;
            case RAW:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                deserializer = (data, out) -> {
                    ArrayData value = data.getArray(fieldPos);
                    ArrayType arrayType = (ArrayType) fieldType;
                    serializeArrayData(value, arrayType.getElementType(), out);
                };
                break;
            case MULTISET:
            case MAP:
                deserializer = (data, out) -> {
                    MapData field = data.getMap(fieldPos);
                    // TODO: 2022/6/15
                };
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int rowFieldCount = getFieldCount(fieldType);
                deserializer = (data, out) -> {
                    RowData field = data.getRow(fieldPos, rowFieldCount);
                    // TODO: 2022/6/15
                };
                break;
            case DISTINCT_TYPE:
                deserializer = (data, out) -> {
                    // TODO: 2022/6/15
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
        return (data, out) -> {
            if (data.isNullAt(fieldPos)) {
                return;
            }
            deserializer.serializer(data, out);
        };
    }


}
