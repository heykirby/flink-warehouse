package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.GenericDataSerializer;
import com.sdu.streaming.warehouse.utils.ByteArrayDataInput;
import com.sdu.streaming.warehouse.utils.ByteArrayDataOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class RedisListTypeSerializer extends AbstractRedisTypeSerializer<byte[][]> {

    public static final RedisListTypeSerializer REDIS_LIST_DESERIALIZER =
            new RedisListTypeSerializer();

    private RedisListTypeSerializer() { }

    @Override
    public byte[][] serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, GenericDataSerializer[] rowFieldSerializers) throws IOException {
        Preconditions.checkArgument(rowFieldGetters.length == rowFieldSerializers.length);

        ByteArrayDataOutput out = new ByteArrayDataOutput();
        byte[][] values = new byte[rowData.getArity()][];
        for (int pos = 0; pos < rowData.getArity(); ++pos) {
            Object fieldValue = rowFieldGetters[pos].getFieldOrNull(rowData);
            rowFieldSerializers[pos].serializer(fieldValue, out);
            values[pos] = out.toByteArray();
            out.reset();
        }
        return values;

    }

    @Override
    public RowData deserializeValue(byte[][] bytes, String[] fieldNames, GenericDataDeserializer[] rowFieldDeserializers) throws IOException {
        Preconditions.checkArgument(bytes.length == fieldNames.length);
        Preconditions.checkArgument(fieldNames.length == rowFieldDeserializers.length);

        GenericRowData rowData = new GenericRowData(fieldNames.length);
        for (int pos = 0; pos < fieldNames.length; ++pos) {
            // TODO: 优化频繁实例化
            ByteArrayDataInput input = new ByteArrayDataInput(bytes[pos]);
            Object fieldValue = rowFieldDeserializers[pos].deserializer(input);
            rowData.setField(pos, fieldValue);
        }

        return rowData;
    }

}
