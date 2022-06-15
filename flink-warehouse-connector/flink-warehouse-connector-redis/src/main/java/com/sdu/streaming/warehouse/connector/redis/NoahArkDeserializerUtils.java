package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

import java.nio.ByteBuffer;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisArrayDataDeserializer.createRedisArrayDataDeserializer;

public class NoahArkDeserializerUtils {

    private NoahArkDeserializerUtils() { }

    public static void serializeStringData(StringData data, ByteBuffer out) {
        byte[] values = data.toBytes();
        out.putInt(values.length);
        out.put(values);
    }

    public static void serializeBooleanData(boolean data, ByteBuffer out) {
        out.putInt(data ? 1 : 0);
    }

    public static void serializeBinaryData(byte[] data, ByteBuffer out) {
        out.putInt(data.length);
        out.put(data);
    }

    public static void serializeDecimalData(DecimalData data, ByteBuffer out) {
        byte[] values = data.toUnscaledBytes();
        out.putInt(values.length);
        out.put(values);
    }

    public static void serializeByteData(byte data, ByteBuffer out) {
        out.put(data);
    }

    public static void serializeShortData(short data, ByteBuffer out) {
        out.putShort(data);
    }

    public static void serializeIntData(int data, ByteBuffer out) {
        out.putInt(data);
    }

    public static void serializeLongData(long data, ByteBuffer out) {
        out.putLong(data);
    }

    public static void serializeDoubleData(double data, ByteBuffer out) {
        out.putDouble(data);
    }

    public static void serializeFloatData(float data, ByteBuffer out) {
        out.putFloat(data);
    }

    public static void serializeTimestampData(TimestampData data, ByteBuffer out) {
        out.putLong(data.getMillisecond());
        out.putLong(data.getNanoOfMillisecond());
    }

    public static void serializeArrayData(ArrayData arrayData, LogicalType elementType, ByteBuffer out) {
        out.putInt(arrayData.size());
        NoahArkRedisArrayDataDeserializer deserializer = createRedisArrayDataDeserializer(elementType);
        for (int arrayIndex = 0; arrayIndex < arrayData.size(); ++arrayIndex) {
            deserializer.serializer(arrayData, arrayIndex, out);
        }
    }

}
