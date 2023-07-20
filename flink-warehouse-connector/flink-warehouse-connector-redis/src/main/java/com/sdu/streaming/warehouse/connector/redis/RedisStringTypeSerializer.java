package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.GenericDataSerializer;
import com.sdu.streaming.warehouse.utils.ByteArrayDataInput;
import com.sdu.streaming.warehouse.utils.ByteArrayDataOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class RedisStringTypeSerializer extends AbstractRedisTypeSerializer<byte[]> {

    public static final RedisStringTypeSerializer REDIS_STRING_DESERIALIZER =
            new RedisStringTypeSerializer();

    private RedisStringTypeSerializer() { }

    @Override
    public byte[] serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, GenericDataSerializer[] rowFieldSerializers) throws IOException {
        Preconditions.checkArgument(rowFieldGetters.length == rowFieldSerializers.length);

        ByteArrayDataOutput out = new ByteArrayDataOutput();
        for (int pos = 0; pos < rowData.getArity(); ++pos) {
            Object fieldValue = rowFieldGetters[pos].getFieldOrNull(rowData);
            rowFieldSerializers[pos].serializer(fieldValue, out);
        }
        return out.toByteArray();
    }

    @Override
    public RowData deserializeValue(byte[] bytes, String[] fieldNames, GenericDataDeserializer[] rowFieldDeserializers) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        Preconditions.checkArgument(fieldNames.length == rowFieldDeserializers.length);
        GenericRowData rowData = new GenericRowData(fieldNames.length);
        ByteArrayDataInput input = new ByteArrayDataInput(bytes);
        for (int pos = 0; pos < fieldNames.length; ++pos) {
            Object fieldValue = rowFieldDeserializers[pos].deserializer(input);
            rowData.setField(pos, fieldValue);
        }
        return rowData;
    }

}
