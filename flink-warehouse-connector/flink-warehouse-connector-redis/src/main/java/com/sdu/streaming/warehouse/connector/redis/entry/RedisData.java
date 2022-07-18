package com.sdu.streaming.warehouse.connector.redis.entry;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.types.RowKind;

import java.util.List;

public interface RedisData<T> {

    long expireTime();

    RowKind getRedisDataKind();

    byte[] getRedisKey();

    T getRedisValue();

    List<RedisFuture<?>> save(StatefulRedisConnection<byte[], byte[]> client);

}
