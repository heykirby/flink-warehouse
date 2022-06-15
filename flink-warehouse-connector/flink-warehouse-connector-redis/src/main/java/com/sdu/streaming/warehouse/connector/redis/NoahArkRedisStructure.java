package com.sdu.streaming.warehouse.connector.redis;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.data.RowData;

// TODO: 存储优化
public enum NoahArkRedisStructure {

    STRING() {

        @Override
        public byte[] serializeValue(RowData rowData, String[] fieldNames, String separator, RowData.FieldGetter[] filedGetters) {
            return serialize(rowData, "", separator, filedGetters);
        }

    },

    LIST() {

        @Override
        public byte[][] serializeValue(RowData rowData, String[] fieldNames, String separator, RowData.FieldGetter[] filedGetters) {
            byte[][] values = new byte[filedGetters.length][];
            for (int i = 0; i < filedGetters.length; ++i) {
                RowData.FieldGetter fieldGetter = filedGetters[i];
                Object field = fieldGetter.getFieldOrNull(rowData);
                values[i] = field == null ? new byte[0] : field.toString().getBytes(StandardCharsets.UTF_8);
            }
            return values;
        }

    },

    MAP() {

        @Override
        public Map<byte[], byte[]> serializeValue(RowData rowData, String[] fieldNames, String separator, RowData.FieldGetter[] filedGetters) {
            Map<byte[], byte[]> values = new HashMap<>();

            int fieldCount = rowData.getArity();
            for (int i = 0; i < fieldCount; ++i) {
                RowData.FieldGetter fieldGetter = filedGetters[i];
                Object field = fieldGetter.getFieldOrNull(rowData);
                values.put(
                        fieldNames[i].getBytes(StandardCharsets.UTF_8),
                        field == null ? new byte[0] : field.toString().getBytes(StandardCharsets.UTF_8)
                );
            }

            return values;
        }

    };

    NoahArkRedisStructure() { }

    public byte[] serializeKey(RowData rowData, String prefix, String separator, RowData.FieldGetter[] keyFiledGetters) {
        return serialize(rowData, prefix, separator, keyFiledGetters);
    }

    public abstract <T> T serializeValue(RowData rowData, String[] fieldNames, String separator, RowData.FieldGetter[] filedGetters);

    protected byte[] serialize(RowData rowData, String prefix, String separator, RowData.FieldGetter[] filedGetters) {
        // 拼接成字符串
        StringBuilder sb = new StringBuilder(prefix);
        boolean first = true;
        for (RowData.FieldGetter field : filedGetters) {
            if (first) {
                first = false;
            } else {
                sb.append(separator);
                sb.append(field.getFieldOrNull(rowData));
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

}
