package com.sdu.streaming.warehouse.connector.redis;

import static org.apache.flink.table.data.RowData.createFieldGetter;

import java.util.Arrays;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;

public class NoahArkRedisRowDataConverter implements NoahArkRedisDataObjectConverter<RowData> {

    private final NoahArkRedisWriteOptions writeOptions;

    private transient RowData.FieldGetter[] primaryKeyFieldGetters;
    private transient RowData.FieldGetter[] rowDataFieldGetters;
    private transient String[] fieldNames;

    public NoahArkRedisRowDataConverter(NoahArkRedisWriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    @Override
    public void open(Configuration cfg) {
        primaryKeyFieldGetters = Arrays.stream(writeOptions.getPrimaryKeyIndexes())
                .mapToObj(index -> createFieldGetter(writeOptions.getRowType().getTypeAt(index), index))
                .toArray(RowData.FieldGetter[]::new);

        int fieldCount = writeOptions.getRowType().getFieldCount();
        rowDataFieldGetters = new RowData.FieldGetter[fieldCount];
        for (int i = 0; i < fieldCount; ++i) {
            rowDataFieldGetters[i] = createFieldGetter(writeOptions.getRowType().getTypeAt(i), i);
        }

        fieldNames = writeOptions.getRowType().getFieldNames().toArray(new String[0]);
    }

    @Override
    public NoahArkRedisDataObject serialize(RowData data) {
        NoahArkRedisStructure structure = writeOptions.getStructure();
        byte[] keys = structure.serializeKey(data, writeOptions.getKeyPrefix(), writeOptions.getKeySeparator(), primaryKeyFieldGetters);
        switch (structure) {
            case MAP:
                Map<byte[], byte[]> mapValues = structure.serializeValue(
                        data,
                        fieldNames,
                        writeOptions.getValueSeparator(),
                        rowDataFieldGetters
                );
                return new NoahArkRedisMapObject(data.getRowKind(), keys, mapValues);

            case LIST:
                byte[][] listValues = structure.serializeValue(
                        data,
                        fieldNames,
                        writeOptions.getValueSeparator(),
                        rowDataFieldGetters
                );
                return new NoahArkRedisListObject(data.getRowKind(), keys, listValues);

            case STRING:
                byte[] stringValues = structure.serializeValue(
                        data,
                        fieldNames,
                        writeOptions.getValueSeparator(),
                        rowDataFieldGetters
                );
                return new NoahArkRedisStringObject(data.getRowKind(), keys, stringValues);

            default:
                throw new UnsupportedOperationException("Unsupported redis structure: " + writeOptions.getStructure());
        }
    }

}
