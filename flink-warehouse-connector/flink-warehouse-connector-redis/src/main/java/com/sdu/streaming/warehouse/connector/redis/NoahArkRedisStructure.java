package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkRowFieldDeserializer;
import com.sdu.streaming.warehouse.utils.NoahArkByteArrayDataOutput;
import org.apache.flink.shaded.guava30.com.google.common.io.ByteArrayDataOutput;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.guava30.com.google.common.io.ByteStreams.newDataOutput;

public enum NoahArkRedisStructure {

    STRING() {

        @Override
        public byte[] serializeValue(RowData rowData, String[] fieldNames, NoahArkRowFieldDeserializer[] deserializers) throws IOException {
            NoahArkByteArrayDataOutput out = new NoahArkByteArrayDataOutput();
            for (int fieldPos = 0; fieldPos < rowData.getArity(); ++fieldPos) {
                NoahArkRowFieldDeserializer deserializer = deserializers[fieldPos];
                deserializer.serializer(rowData, fieldPos, out);
            }
            return out.toByteArray();
        }

    },

    LIST() {

        @Override
        public byte[][] serializeValue(RowData rowData, String[] fieldNames, NoahArkRowFieldDeserializer[] deserializers) throws IOException {
            NoahArkByteArrayDataOutput out = new NoahArkByteArrayDataOutput();
            byte[][] values = new byte[rowData.getArity()][];
            for (int fieldPos = 0; fieldPos < rowData.getArity(); ++fieldPos) {
                NoahArkRowFieldDeserializer deserializer = deserializers[fieldPos];
                deserializer.serializer(rowData, fieldPos, out);
                values[fieldPos] = out.toByteArray();
                out.reset();
            }
            return values;
        }

    },

    MAP() {

        @Override
        public Map<byte[], byte[]> serializeValue(RowData rowData, String[] fieldNames, NoahArkRowFieldDeserializer[] deserializers) throws IOException {
            NoahArkByteArrayDataOutput out = new NoahArkByteArrayDataOutput();
            Map<byte[], byte[]> values = new HashMap<>();
            int fieldCount = rowData.getArity();
            for (int fieldPos = 0; fieldPos < fieldCount; ++fieldPos) {
                NoahArkRowFieldDeserializer deserializer = deserializers[fieldPos];
                deserializer.serializer(rowData, fieldPos, out);
                values.put(
                        fieldNames[fieldPos].getBytes(StandardCharsets.UTF_8),
                        out.toByteArray()
                );
                out.reset();
            }

            return values;
        }

    };

    NoahArkRedisStructure() { }

    public byte[] serializeKey(RowData rowData, String prefix, NoahArkRowFieldDeserializer[] keyDeserializers) throws IOException {
        return serialize(rowData, prefix, keyDeserializers);
    }

    public abstract <T> T serializeValue(RowData rowData,
                                         String[] fieldNames,
                                         NoahArkRowFieldDeserializer[] deserializer) throws IOException;

    protected byte[] serialize(RowData rowData, String prefix, NoahArkRowFieldDeserializer[] deserializers) throws IOException {
        ByteArrayDataOutput out = newDataOutput();
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        out.writeInt(prefixBytes.length);
        out.write(prefixBytes);
        for (int fieldPos = 0; fieldPos < rowData.getArity(); ++fieldPos) {
            NoahArkRowFieldDeserializer deserializer = deserializers[fieldPos];
            deserializer.serializer(rowData, fieldPos, out);
        }
        return out.toByteArray();
    }

}
