package com.sdu.streaming.warehouse.deserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import static com.sdu.streaming.warehouse.deserializer.NoahArkDataDeserializer.createDataDeserializer;
import static com.sdu.streaming.warehouse.deserializer.NoahArkDataSerializer.createDataSerializer;

public class NoahArkDataDeserializerUtils {

    private NoahArkDataDeserializerUtils() { }

    public static void serializeStringData(StringData data, DataOutput out) throws IOException {
        byte[] values = data.toBytes();
        out.writeInt(values.length);
        out.write(values);
    }

    public static StringData deserializeStringData(DataInput input) throws IOException {
        int length = input.readInt();
        byte[] values = new byte[length];
        input.readFully(values);
        return StringData.fromBytes(values);
    }

    public static void serializeBooleanData(boolean data, DataOutput out) throws IOException {
        out.writeBoolean(data);
    }

    public static boolean deserializeBooleanData(DataInput input) throws IOException {
        return input.readBoolean();
    }

    public static void serializeBinaryData(byte[] data, DataOutput out) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    public static BinaryArrayData deserializeBinaryData(DataInput input) throws IOException {
        int length = input.readInt();
        byte[] values = new byte[length];
        input.readFully(values);
        return BinaryArrayData.fromPrimitiveArray(values);
    }

    public static void serializeDecimalData(DecimalData data, DataOutput out) throws IOException {
        byte[] values = data.toUnscaledBytes();
        out.writeInt(data.precision());
        out.writeInt(data.scale());
        out.writeInt(values.length);
        out.write(values);
    }

    public static DecimalData deserializeDecimalData(DataInput input) throws IOException {
        int precision = input.readInt();
        int scale = input.readInt();
        int length = input.readInt();
        byte[] values = new byte[length];
        input.readFully(values);
        return DecimalData.fromUnscaledBytes(values, precision, scale);
    }

    public static void serializeByteData(byte data, DataOutput out) throws IOException {
        out.writeByte(data);
    }

    public static byte deserializeByteData(DataInput input) throws IOException {
        return input.readByte();
    }

    public static void serializeShortData(short data, DataOutput out) throws IOException {
        out.writeShort(data);
    }

    public static short deserializeShortData(DataInput input) throws IOException {
        return input.readShort();
    }

    public static void serializeIntData(int data, DataOutput out) throws IOException {
        out.writeInt(data);
    }

    public static int deserializeIntData(DataInput input) throws IOException {
        return input.readInt();
    }

    public static void serializeLongData(long data, DataOutput out) throws IOException {
        out.writeLong(data);
    }

    public static long deserializeLongData(DataInput input) throws IOException {
        return input.readLong();
    }

    public static void serializeDoubleData(double data, DataOutput out) throws IOException {
        out.writeDouble(data);
    }

    public static double deserializeDoubleData(DataInput input) throws IOException {
        return input.readDouble();
    }

    public static void serializeFloatData(float data, DataOutput out) throws IOException {
        out.writeFloat(data);
    }

    public static float deserializeFloatData(DataInput input) throws IOException {
        return input.readFloat();
    }

    public static void serializeTimestampData(TimestampData data, DataOutput out) throws IOException {
        out.writeLong(data.getMillisecond());
        out.writeInt(data.getNanoOfMillisecond());
    }

    public static TimestampData deserializeTimestampData(DataInput input) throws IOException {
        return TimestampData.fromEpochMillis(input.readLong(), input.readInt());
    }

    public static void serializeArrayData(ArrayData data, LogicalType elementType, DataOutput out) throws IOException {
        out.writeInt(data.size());
        NoahArkDataSerializer serializer = createDataSerializer(elementType);
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        for (int arrayIndex = 0; arrayIndex < data.size(); ++arrayIndex) {
            Object element = elementGetter.getElementOrNull(data, arrayIndex);
            serializer.serializer(element, out);
        }
    }

    public static ArrayData deserializeArrayData(DataInput input, LogicalType elementType) throws IOException {
        int arraySize = input.readInt();
        NoahArkDataDeserializer deserializer = createDataDeserializer(elementType);
        Object[] arrayData = new Object[arraySize];
        for (int arrayIndex = 0; arrayIndex < arraySize; ++arrayIndex) {
            arrayData[arrayIndex] = deserializer.deserializer(input);
        }
        return new GenericArrayData(arrayData);
    }

    public static void serializeMapData(MapData data, LogicalType keyType, LogicalType valueType, DataOutput out) throws IOException {
        serializeArrayData(data.keyArray(), keyType, out);
        serializeArrayData(data.valueArray(), valueType, out);
    }

    public static MapData deserializeMapData(DataInput input, LogicalType keyType, LogicalType valueType) throws IOException {
        ArrayData keyArray = deserializeArrayData(input, keyType);
        ArrayData.ElementGetter keyElementGetter = ArrayData.createElementGetter(keyType);
        ArrayData valueArray = deserializeArrayData(input, valueType);
        ArrayData.ElementGetter valueElementGetter = ArrayData.createElementGetter(valueType);

        Preconditions.checkArgument(keyArray.size() == valueArray.size());

        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < keyArray.size(); ++i) {
            Object key = keyElementGetter.getElementOrNull(keyArray, i);
            Object value = valueElementGetter.getElementOrNull(valueArray, i);
            map.put(key, value);
        }

        return new GenericMapData(map);
    }

    public static void serializeRowData(RowData data, RowType rowType, DataOutput out) throws IOException {
        out.writeInt(rowType.getFieldCount());
        out.writeByte(data.getRowKind().toByteValue());
        int index = 0;
        for (RowType.RowField field : rowType.getFields()) {
            RowData.FieldGetter fieldGetter = RowData.createFieldGetter(field.getType(), index);
            NoahArkDataSerializer deserializer = createDataSerializer(field.getType());
            deserializer.serializer(fieldGetter.getFieldOrNull(data), out);
            index += 1;
        }
    }

    public static RowData deserializeRowData(DataInput input, RowType rowType) throws IOException {
        int fieldCount = input.readInt();
        RowKind kind = RowKind.fromByteValue(input.readByte());
        GenericRowData rowData = new GenericRowData(kind, fieldCount);
        int index = 0;
        for (RowType.RowField field : rowType.getFields()) {
            NoahArkDataDeserializer deserializer = createDataDeserializer(field.getType());
            rowData.setField(index, deserializer.deserializer(input));
            index += 1;
        }
        return rowData;
    }

}
