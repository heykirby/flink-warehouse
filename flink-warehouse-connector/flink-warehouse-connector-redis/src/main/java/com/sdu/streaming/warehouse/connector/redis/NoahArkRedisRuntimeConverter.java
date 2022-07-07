package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisData;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.BiConsumer;

public interface NoahArkRedisRuntimeConverter<T> extends Serializable {

    void open() throws IOException;

    NoahArkRedisData<?> serialize(T data) throws IOException;

    T deserialize(StatefulRedisClusterConnection<byte[], byte[]> client, RowData key) throws IOException;

    void asyncDeserialize(StatefulRedisClusterConnection<byte[], byte[]> client, RowData key, BiConsumer<T, Throwable> resultConsumer) throws IOException;
}
