package com.sdu.streaming.warehouse.connector.redis.entry;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.types.RowKind;

public interface NoahArkRedisData<T> {

    long expireTime();

    RowKind getRedisDataKind();

    byte[] getRedisKey();

    T getRedisValue();

    void save(StatefulRedisClusterConnection<byte[], byte[]> client);

}
