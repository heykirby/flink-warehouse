package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.NoahArkDataSerializer;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisListTypeSerializer.REDIS_LIST_DESERIALIZER;
import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisMapTypeSerializer.REDIS_MAP_DESERIALIZER;
import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisStringTypeSerializer.REDIS_STRING_DESERIALIZER;


public class NoahArkRedisRowDataRuntimeConverter implements NoahArkRedisRuntimeConverter<RowData> {

    private final NoahArkRedisOptions redisOptions;

    // primary key
    private transient NoahArkDataSerializer[] rowKeySerializers;
    private transient RowData.FieldGetter[] rowKeyFieldGetters;

    // row field
    private transient NoahArkDataSerializer[] rowFieldSerializers;
    private transient RowData.FieldGetter[] rowFieldGetters;

    private transient NoahArkDataDeserializer[] rowFieldDeserializers;
    private transient String[] fieldNames;

    public NoahArkRedisRowDataRuntimeConverter(NoahArkRedisOptions redisOptions) {
        this.redisOptions = redisOptions;
    }

    @Override
    public void open() {
        RowType rowType = redisOptions.getRowType();
        int[] primaryKeyIndexes = redisOptions.getPrimaryKeyIndexes();
        // TODO: 读写不同
//        rowKeySerializers = new NoahArkDataSerializer[primaryKeyIndexes.length];
//        for (int index = 0; index < primaryKeyIndexes.length; ++index) {
//            int primaryKeyFieldPos = primaryKeyIndexes[index];
//            rowKeySerializers[index] = createDataSerializer(rowType.getTypeAt(primaryKeyFieldPos));
//        }
//
//        rowDataSerializer = createDataSerializer(rowType);
//        rowDataDeserializer = createDataDeserializer(rowType);
//        fieldNames = redisOptions.getRowType().getFieldNames().toArray(new String[0]);
    }

    @Override
    public NoahArkRedisObject serialize(RowData data) throws IOException {
        NoahArkRedisDataType redisDataType = redisOptions.getRedisDataType();
        switch (redisDataType) {
            case MAP:
                byte[] mapKeys = REDIS_MAP_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                Map<byte[], byte[]> mapValues = REDIS_MAP_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new NoahArkRedisMapObject(data.getRowKind(), mapKeys, mapValues);

            case LIST:
                byte[] listKeys = REDIS_LIST_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                byte[][] listValues = REDIS_LIST_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new NoahArkRedisListObject(data.getRowKind(), listKeys, listValues);

            case STRING:
                byte[] stringKeys = REDIS_STRING_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                byte[] stringValues = REDIS_STRING_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new NoahArkRedisStringObject(data.getRowKind(), stringKeys, stringValues);

            default:
                throw new UnsupportedOperationException("Unsupported redis data type: " + redisOptions.getRedisDataType());
        }
    }

    @Override
    public RowData deserialize(StatefulRedisClusterConnection<byte[], byte[]> client, RowData key) throws IOException {
        NoahArkRedisDataType redisDataType = redisOptions.getRedisDataType();
        String keyPrefix = redisOptions.getKeyPrefix();
        switch (redisDataType) {
            case MAP:
                byte[] mapKeys = REDIS_MAP_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                Map<byte[], byte[]> mapValues = client.sync().hgetall(mapKeys);
                return REDIS_MAP_DESERIALIZER.deserializeValue(mapValues, fieldNames, rowFieldDeserializers);

            case LIST:
                byte[] listKeys = REDIS_LIST_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                List<byte[]> listValues = client.sync().lrange(listKeys, 0, -1);
                return REDIS_LIST_DESERIALIZER.deserializeValue(listValues.toArray(new byte[0][0]), fieldNames, rowFieldDeserializers);

            case STRING:
                byte[] stringKeys = REDIS_STRING_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                byte[] stringValues = client.sync().get(stringKeys);
                return REDIS_STRING_DESERIALIZER.deserializeValue(stringValues, fieldNames, rowFieldDeserializers);

            default:
                throw new UnsupportedOperationException("Unsupported redis data type: " + redisOptions.getRedisDataType());
        }
    }

}
