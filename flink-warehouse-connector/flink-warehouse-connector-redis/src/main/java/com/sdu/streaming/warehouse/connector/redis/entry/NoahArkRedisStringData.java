package com.sdu.streaming.warehouse.connector.redis.entry;

import com.sdu.streaming.warehouse.connector.redis.NoahArkAbstractRedisData;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.types.RowKind;

public class NoahArkRedisStringData extends NoahArkAbstractRedisData<byte[]> {

    public NoahArkRedisStringData(long expireTime, RowKind kind, byte[] keys, byte[] values) {
        super(expireTime, kind, keys, values);
    }

    @Override
    public void save(StatefulRedisConnection<byte[], byte[]> client) {
        RedisCommands<byte[], byte[]> command = client.sync();
        switch (getRedisDataKind()) {
            case INSERT:
            case UPDATE_AFTER:
                command.set(getRedisKey(), getRedisValue());
                command.expire(getRedisKey(), expireTime());
                break;

            case DELETE:
            case UPDATE_BEFORE:
                command.del(getRedisKey());
                break;
        }
    }
}
