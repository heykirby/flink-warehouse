package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkRowFieldDeserializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.Map;

import static com.sdu.streaming.warehouse.deserializer.NoahArkRowFieldDeserializer.createRowFieldDeserializer;

public class NoahArkRedisRowDataConverter implements NoahArkRedisDataObjectConverter<RowData> {

    private final NoahArkRedisWriteOptions writeOptions;

    private transient NoahArkRowFieldDeserializer[] primaryKeyFieldDeserializers;
    private transient NoahArkRowFieldDeserializer[] rowFieldDeserializers;
    private transient String[] fieldNames;

    public NoahArkRedisRowDataConverter(NoahArkRedisWriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    @Override
    public void open(Configuration cfg) {
        RowType rowType = writeOptions.getRowType();
        int[] primaryKeyIndexes = writeOptions.getPrimaryKeyIndexes();
        primaryKeyFieldDeserializers = new NoahArkRowFieldDeserializer[primaryKeyIndexes.length];
        for (int index = 0; index < primaryKeyIndexes.length; ++index) {
            int primaryKeyFieldPos = primaryKeyIndexes[index];
            primaryKeyFieldDeserializers[index] =
                    createRowFieldDeserializer(rowType.getTypeAt(primaryKeyFieldPos));
        }

        rowFieldDeserializers = new NoahArkRowFieldDeserializer[rowType.getFieldCount()];
        for (int fieldPos = 0; fieldPos < rowType.getFieldCount(); ++fieldPos) {
            rowFieldDeserializers[fieldPos] =
                    createRowFieldDeserializer(rowType.getTypeAt(fieldPos));
        }
        fieldNames = writeOptions.getRowType().getFieldNames().toArray(new String[0]);
    }

    @Override
    public NoahArkRedisDataObject serialize(RowData data) throws IOException  {
        NoahArkRedisStructure structure = writeOptions.getStructure();
        byte[] keys = structure.serializeKey(data, writeOptions.getKeyPrefix(), primaryKeyFieldDeserializers);
        switch (structure) {
            case MAP:
                Map<byte[], byte[]> mapValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowFieldDeserializers
                );
                return new NoahArkRedisMapObject(data.getRowKind(), keys, mapValues);

            case LIST:
                byte[][] listValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowFieldDeserializers
                );
                return new NoahArkRedisListObject(data.getRowKind(), keys, listValues);

            case STRING:
                byte[] stringValues = structure.serializeValue(
                        data,
                        fieldNames,
                        rowFieldDeserializers
                );
                return new NoahArkRedisStringObject(data.getRowKind(), keys, stringValues);

            default:
                throw new UnsupportedOperationException("Unsupported redis structure: " + writeOptions.getStructure());
        }
    }

}
