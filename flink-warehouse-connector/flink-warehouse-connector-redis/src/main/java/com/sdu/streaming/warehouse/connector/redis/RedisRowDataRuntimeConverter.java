package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.*;
import com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.GenericDataSerializer;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.sdu.streaming.warehouse.connector.redis.RedisListTypeSerializer.REDIS_LIST_DESERIALIZER;
import static com.sdu.streaming.warehouse.connector.redis.RedisMapTypeSerializer.REDIS_MAP_DESERIALIZER;
import static com.sdu.streaming.warehouse.connector.redis.RedisStringTypeSerializer.REDIS_STRING_DESERIALIZER;
import static com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer.createDataDeserializer;
import static com.sdu.streaming.warehouse.deserializer.GenericDataSerializer.createDataSerializer;


public class RedisRowDataRuntimeConverter implements RedisRuntimeConverter<RowData> {

    // read:
    //  primaryKeyIndexes[i][0]: 关联条件索引位置
    //  primaryKeyIndexes[i][1]: RowType中索引位置
    // write:
    //  primaryKeyIndexes[i][0]: RowType中索引位置
    //  primaryKeyIndexes[i][1]: RowType中索引位置
    private final int[][] primaryKeyIndexes;
    private final RedisOptions redisOptions;

    // primary key
    private transient GenericDataSerializer[] rowKeySerializers;
    private transient RowData.FieldGetter[] rowKeyFieldGetters;

    // row field
    private transient GenericDataSerializer[] rowFieldSerializers;
    private transient RowData.FieldGetter[] rowFieldGetters;

    private transient GenericDataDeserializer[] rowFieldDeserializers;
    private transient String[] fieldNames;

    public RedisRowDataRuntimeConverter(RedisOptions redisOptions, int[][] primaryKeyIndexes) {
        this.redisOptions = redisOptions;
        this.primaryKeyIndexes = primaryKeyIndexes;
    }

    @Override
    public void open() throws IOException {
        RowType rowType = redisOptions.getRowType();

        // primary key
        rowKeySerializers = new GenericDataSerializer[primaryKeyIndexes.length];
        rowKeyFieldGetters = new RowData.FieldGetter[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; ++i) {
            LogicalType rowKeyType = rowType.getTypeAt(primaryKeyIndexes[i][1]);
            rowKeySerializers[i] = createDataSerializer(rowKeyType);
            rowKeyFieldGetters[i] = RowData.createFieldGetter(rowKeyType, primaryKeyIndexes[i][0]);
        }

        // write
        rowFieldSerializers = new GenericDataSerializer[rowType.getFieldCount()];
        rowFieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            rowFieldSerializers[i] = createDataSerializer(rowType.getTypeAt(i));
            rowFieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }

        // read
        rowFieldDeserializers = new GenericDataDeserializer[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            rowFieldDeserializers[i] = createDataDeserializer(rowType.getTypeAt(i));
        }

        fieldNames = rowType.getFieldNames().toArray(new String[0]);
    }

    @Override
    public RedisData<?> serialize(RowData data) throws IOException {
        RedisDataType redisDataType = redisOptions.getRedisDataType();
        long expireSeconds = redisOptions.expireTime();
        switch (redisDataType) {
            case MAP:
                byte[] mapKeys = REDIS_MAP_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                Map<byte[], byte[]> mapValues = REDIS_MAP_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new RedisMapData(expireSeconds, data.getRowKind(), mapKeys, mapValues);

            case LIST:
                byte[] listKeys = REDIS_LIST_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                byte[][] listValues = REDIS_LIST_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new RedisListData(expireSeconds, data.getRowKind(), listKeys, listValues);

            case STRING:
                byte[] stringKeys = REDIS_STRING_DESERIALIZER.serializeKey(data, redisOptions.getKeyPrefix(), rowKeyFieldGetters, rowKeySerializers);
                byte[] stringValues = REDIS_STRING_DESERIALIZER.serializeValue(
                        data,
                        fieldNames,
                        rowFieldGetters,
                        rowFieldSerializers
                );
                return new RedisStringData(expireSeconds, data.getRowKind(), stringKeys, stringValues);

            default:
                throw new UnsupportedOperationException("Unsupported redis data type: " + redisOptions.getRedisDataType());
        }
    }

    @Override
    public RowData deserialize(StatefulRedisConnection<byte[], byte[]> client, RowData key) throws IOException {
        RedisDataType redisDataType = redisOptions.getRedisDataType();
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

    @Override
    public void asyncDeserialize(StatefulRedisConnection<byte[], byte[]> client, RowData key, BiConsumer<RowData, Throwable> resultConsumer) throws IOException{
        RedisDataType redisDataType = redisOptions.getRedisDataType();
        String keyPrefix = redisOptions.getKeyPrefix();
        switch (redisDataType) {
            case MAP:
                byte[] mapKeys = REDIS_MAP_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                client.async().hgetall(mapKeys).whenComplete((mapValues, throwable) -> {
                            if (throwable != null) {
                                resultConsumer.accept(null, throwable);
                            } else {
                                try {
                                    RowData valueRow = REDIS_MAP_DESERIALIZER.deserializeValue(mapValues, fieldNames, rowFieldDeserializers);
                                    resultConsumer.accept(valueRow, null);
                                } catch (IOException ex) {
                                    resultConsumer.accept(null, ex);
                                }

                            }
                        });

            case LIST:
                byte[] listKeys = REDIS_LIST_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                client.async().lrange(listKeys, 0, -1).whenComplete((listValues, throwable) -> {
                    if (throwable != null) {
                        resultConsumer.accept(null, throwable);
                    } else {
                        try {
                            RowData valueRow = REDIS_LIST_DESERIALIZER.deserializeValue(listValues.toArray(new byte[0][0]), fieldNames, rowFieldDeserializers);
                            resultConsumer.accept(valueRow, null);
                        } catch (IOException ex) {
                            resultConsumer.accept(null, ex);
                        }
                    }
                });

            case STRING:
                byte[] stringKeys = REDIS_STRING_DESERIALIZER.serializeKey(key, keyPrefix, rowKeyFieldGetters, rowKeySerializers);
                client.async().get(stringKeys).whenComplete((stringValues, throwable) -> {
                    if (throwable != null) {
                        resultConsumer.accept(null, throwable);
                    } else {
                        try {
                            RowData valueRow = REDIS_STRING_DESERIALIZER.deserializeValue(stringValues, fieldNames, rowFieldDeserializers);
                            resultConsumer.accept(valueRow, null);
                        } catch (IOException ex) {
                            resultConsumer.accept(null, ex);
                        }
                    }
                });

            default:
                throw new UnsupportedOperationException("Unsupported redis data type: " + redisOptions.getRedisDataType());
        }
    }

}
