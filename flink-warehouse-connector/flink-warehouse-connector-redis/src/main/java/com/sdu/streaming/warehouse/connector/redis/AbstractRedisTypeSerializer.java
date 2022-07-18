package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkDataSerializer;
import com.sdu.streaming.warehouse.utils.ByteArrayDataOutput;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class AbstractRedisTypeSerializer<T> implements RedisTypeSerializer<T> {

    @Override
    public byte[] serializeKey(RowData rowData, String prefix, RowData.FieldGetter[] keyFieldGetters, NoahArkDataSerializer[] rowKeySerializers) throws IOException {
        Preconditions.checkArgument(keyFieldGetters.length == rowKeySerializers.length);
        ByteArrayDataOutput out = new ByteArrayDataOutput();
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        out.writeInt(prefixBytes.length);
        out.write(prefixBytes);
        for (int index = 0; index < keyFieldGetters.length; ++index) {
            Object keyField = keyFieldGetters[index].getFieldOrNull(rowData);
            rowKeySerializers[index].serializer(keyField, out);
        }
        return out.toByteArray();
    }
}
