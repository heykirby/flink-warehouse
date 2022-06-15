package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Map;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRowDataDeserializer.createRedisRowDataDeserializer;

public class NoahArkRedisRowDataConverter implements NoahArkRedisDataObjectConverter<RowData> {

    private final NoahArkRedisWriteOptions writeOptions;

    private transient NoahArkRedisRowDataDeserializer[] primaryKeyFieldGetters;
    private transient NoahArkRedisRowDataDeserializer[] rowDataFieldGetters;
    private transient String[] fieldNames;

    public NoahArkRedisRowDataConverter(NoahArkRedisWriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    @Override
    public void open(Configuration cfg) {
        RowType rowType = writeOptions.getRowType();
        int primaryKeyLength = writeOptions.getPrimaryKeyIndexes().length;
        primaryKeyFieldGetters = new NoahArkRedisRowDataDeserializer[primaryKeyLength];
        for (int index = 0; index < primaryKeyLength; ++index) {
            int primaryKeyFieldPos = writeOptions.getPrimaryKeyIndexes()[index];
            primaryKeyFieldGetters[index] =
                    createRedisRowDataDeserializer(rowType.getTypeAt(primaryKeyFieldPos));
        }

        rowDataFieldGetters = new NoahArkRedisRowDataDeserializer[rowType.getFieldCount()];
        for (int fieldPos = 0; fieldPos < rowType.getFieldCount(); ++fieldPos) {
            rowDataFieldGetters[fieldPos] =
                    createRedisRowDataDeserializer(rowType.getTypeAt(fieldPos));
        }
        fieldNames = writeOptions.getRowType().getFieldNames().toArray(new String[0]);
    }

    @Override
    public NoahArkRedisDataObject serialize(RowData data) throws IOException  {
        NoahArkRedisStructure structure = writeOptions.getStructure();
        byte[] keys = structure.serializeKey(data, writeOptions.getKeyPrefix(), primaryKeyFieldGetters);
        switch (structure) {
            case MAP:
                Map<byte[], byte[]> mapValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowDataFieldGetters
                );
                return new NoahArkRedisMapObject(data.getRowKind(), keys, mapValues);

            case LIST:
                byte[][] listValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowDataFieldGetters
                );
                return new NoahArkRedisListObject(data.getRowKind(), keys, listValues);

            case STRING:
                byte[] stringValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowDataFieldGetters
                );
                return new NoahArkRedisStringObject(data.getRowKind(), keys, stringValues);

            default:
                throw new UnsupportedOperationException("Unsupported redis structure: " + writeOptions.getStructure());
        }
    }

}
