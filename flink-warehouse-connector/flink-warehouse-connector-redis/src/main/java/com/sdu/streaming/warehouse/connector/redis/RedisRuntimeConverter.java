package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.RedisData;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.BiConsumer;

public interface RedisRuntimeConverter<T> extends Serializable {

    void open() throws IOException;

    RedisData<?> serialize(T data) throws IOException;

    T deserialize(StatefulRedisConnection<byte[], byte[]> client, RowData key) throws IOException;

    void asyncDeserialize(StatefulRedisConnection<byte[], byte[]> client, RowData key, BiConsumer<T, Throwable> resultConsumer) throws IOException;
}
