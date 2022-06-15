package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.*;

public interface NoahArkRedisDataObjectDeserializer extends Serializable {

    void serializer(RowData data, ByteBuffer out);


    static NoahArkRedisDataObjectDeserializer createRedisDataObjectDeserializer(LogicalType fieldType, int fieldPos) {
        NoahArkRedisDataObjectDeserializer deserializer;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                deserializer = (data, out) -> {
                    StringData field = data.getString(fieldPos);
                    byte[] values = field.toBytes();
                    out.putInt(values.length);
                    out.put(values);
                };
                break;
            case BOOLEAN:
                deserializer = (data, out) -> {
                    boolean value = data.getBoolean(fieldPos);
                    out.putInt(value ? 1 : 0);
                };
                break;
            case BINARY:
            case VARBINARY:
                deserializer = (data, out) -> {
                    byte[] values = data.getBinary(fieldPos);
                    out.putInt(values.length);
                    out.put(values);
                };
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                deserializer = (data, out) -> {
                    DecimalData field = data.getDecimal(fieldPos, decimalPrecision, decimalScale);
                    byte[] values = field.toUnscaledBytes();
                    out.putInt(values.length);
                    out.put(values);
                };
                break;
            case TINYINT:
                deserializer = (data, out) -> {
                    byte value = data.getByte(fieldPos);
                    out.put(value);
                };
                break;
            case SMALLINT:
                deserializer = (data, out) -> {
                    short value = data.getShort(fieldPos);
                    out.putShort(value);
                };
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                deserializer = (data, out) -> {
                    int value = data.getInt(fieldPos);
                    out.putInt(value);
                };
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                deserializer = (data, out) -> {
                    long value = data.getLong(fieldPos);
                    out.putLong(value);
                };
                break;
            case FLOAT:
                deserializer = (data, out) -> {
                    float value = data.getFloat(fieldPos);
                    out.putFloat(value);
                };
                break;
            case DOUBLE:
                deserializer = (data, out) -> {
                    double value = data.getDouble(fieldPos);
                    out.putDouble(value);
                };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                deserializer = (data, out) -> {
                    TimestampData value = data.getTimestamp(fieldPos, timestampPrecision);
                    out.putLong(value.getMillisecond());
                    out.putLong(value.getNanoOfMillisecond());
                };
                break;
            case RAW:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                deserializer = (data, out) -> {
                    ArrayData field = data.getArray(fieldPos);
                    // TODO: 2022/6/15
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
