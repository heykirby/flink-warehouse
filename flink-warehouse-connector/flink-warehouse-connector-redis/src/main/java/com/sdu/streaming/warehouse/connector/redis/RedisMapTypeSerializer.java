package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.GenericDataSerializer;
import com.sdu.streaming.warehouse.utils.ByteArrayDataInput;
import com.sdu.streaming.warehouse.utils.ByteArrayDataOutput;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class RedisMapTypeSerializer extends AbstractRedisTypeSerializer<Map<byte[], byte[]>> {

    public static final RedisMapTypeSerializer REDIS_MAP_DESERIALIZER =
            new RedisMapTypeSerializer();

    private RedisMapTypeSerializer() { }

    @Override
    public Map<byte[], byte[]> serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, GenericDataSerializer[] rowFieldSerializers) throws IOException {
        Preconditions.checkArgument(rowFieldGetters.length == fieldNames.length);
        Preconditions.checkArgument(rowFieldGetters.length == rowFieldSerializers.length);

        ByteArrayDataOutput out = new ByteArrayDataOutput();
        Map<byte[], byte[]> values = new HashMap<>();
        int arity = rowData.getArity();
        for (int pos = 0; pos < arity; ++pos) {
            byte[] key = fieldNames[pos].getBytes(StandardCharsets.UTF_8);
            Object fieldValue = rowFieldGetters[pos].getFieldOrNull(rowData);
            rowFieldSerializers[pos].serializer(fieldValue, out);
            byte[] value = out.toByteArray();
            values.put(key, value);
            out.reset();
        }

        return values;
    }

    @Override
    public RowData deserializeValue(Map<byte[], byte[]> bytes, String[] fieldNames, GenericDataDeserializer[] rowFieldDeserializers) throws IOException {
        Preconditions.checkArgument(bytes.keySet().size() == fieldNames.length);
        Preconditions.checkArgument(fieldNames.length == rowFieldDeserializers.length);

        GenericRowData rowData = new GenericRowData(fieldNames.length);
        for (int pos = 0; pos < fieldNames.length; ++pos) {
            byte[] key = fieldNames[pos].getBytes(StandardCharsets.UTF_8);
            byte[] value = bytes.get(key);
            // TODO: 优化频繁实例化
            ByteArrayDataInput input = new ByteArrayDataInput(value);
            Object fieldValue = rowFieldDeserializers[pos].deserializer(input);
            rowData.setField(pos, fieldValue);
        }
        return rowData;
    }
}
