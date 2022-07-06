package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.NoahArkDataSerializer;
import com.sdu.streaming.warehouse.utils.NoahArkByteArrayDataOutput;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class NoahArkRedisListTypeSerializer extends NoahArkAbstractRedisTypeSerializer<byte[][]> {

    public static final NoahArkRedisListTypeSerializer REDIS_LIST_DESERIALIZER =
            new NoahArkRedisListTypeSerializer();

    private NoahArkRedisListTypeSerializer() { }

    @Override
    public byte[][] serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, NoahArkDataSerializer[] rowFieldSerializers) throws IOException {
        Preconditions.checkArgument(rowFieldGetters.length == rowFieldSerializers.length);

        NoahArkByteArrayDataOutput out = new NoahArkByteArrayDataOutput();
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
    public RowData deserializeValue(byte[][] bytes, String[] fieldNames, NoahArkDataDeserializer[] rowFieldDeserializers) throws IOException {
        return null;
    }

}
