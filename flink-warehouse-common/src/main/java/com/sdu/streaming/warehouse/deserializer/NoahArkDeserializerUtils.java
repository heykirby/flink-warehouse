package com.sdu.streaming.warehouse.deserializer;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.DataOutput;
import java.io.IOException;

import static com.sdu.streaming.warehouse.deserializer.NoahArkArrayElementDeserializer.createArrayElementDeserializer;

public class NoahArkDeserializerUtils {

    private NoahArkDeserializerUtils() { }

    public static void serializeStringData(StringData data, DataOutput out) throws IOException {
        byte[] values = data.toBytes();
        out.writeInt(values.length);
        out.write(values);
    }

    public static void serializeBooleanData(boolean data, DataOutput out) throws IOException {
        out.writeBoolean(data);
    }

    public static void serializeBinaryData(byte[] data, DataOutput out) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    public static void serializeDecimalData(DecimalData data, DataOutput out) throws IOException {
        byte[] values = data.toUnscaledBytes();
        out.writeInt(values.length);
        out.write(values);
    }

    public static void serializeByteData(byte data, DataOutput out) throws IOException {
        out.writeByte(data);
    }

    public static void serializeShortData(short data, DataOutput out) throws IOException {
        out.writeShort(data);
    }

    public static void serializeIntData(int data, DataOutput out) throws IOException {
        out.writeInt(data);
    }

    public static void serializeLongData(long data, DataOutput out) throws IOException {
        out.writeLong(data);
    }

    public static void serializeDoubleData(double data, DataOutput out) throws IOException {
        out.writeDouble(data);
    }

    public static void serializeFloatData(float data, DataOutput out) throws IOException {
        out.writeFloat(data);
    }

    public static void serializeTimestampData(TimestampData data, DataOutput out) throws IOException {
        out.writeLong(data.getMillisecond());
        out.writeLong(data.getNanoOfMillisecond());
    }

    public static void serializeArrayData(ArrayData data, LogicalType elementType, DataOutput out) throws IOException {
        out.writeInt(data.size());
        NoahArkArrayElementDeserializer deserializer = createArrayElementDeserializer(elementType);
        for (int arrayIndex = 0; arrayIndex < data.size(); ++arrayIndex) {
            deserializer.serializer(data, arrayIndex, out);
        }
    }

    public static void serializeMapData(MapData data, LogicalType keyType, LogicalType valueType, DataOutput out) throws IOException {
        serializeArrayData(data.keyArray(), keyType, out);
        serializeArrayData(data.valueArray(), valueType, out);
    }

    public static void serializeRowData(RowData data, RowType rowType, DataOutput out) throws IOException {
        int index = 0;
        for (RowType.RowField field : rowType.getFields()) {
            NoahArkRowFieldDeserializer deserializer = NoahArkRowFieldDeserializer.createRowFieldDeserializer(field.getType());
            deserializer.serializer(data, index, out);
            index += 1;
        }
    }


}
