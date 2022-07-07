package com.sdu.streaming.warehouse.connector.redis.entry;

import com.sdu.streaming.warehouse.connector.redis.NoahArkAbstractRedisData;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.flink.types.RowKind;

import java.util.Map;

public class NoahArkRedisMapData extends NoahArkAbstractRedisData<Map<byte[], byte[]>> {

    public NoahArkRedisMapData(long expireTime, RowKind kind, byte[] keys, Map<byte[], byte[]> values) {
        super(expireTime, kind, keys, values);
    }

    @Override
    public void save(StatefulRedisClusterConnection<byte[], byte[]> client) {
        RedisAdvancedClusterCommands<byte[], byte[]> command = client.sync();
        switch (getRedisDataKind()) {
            case INSERT:
            case UPDATE_AFTER:
                command.hmset(getRedisKey(), getRedisValue());
                command.expire(getRedisKey(), expireTime());
                break;

            case DELETE:
            case UPDATE_BEFORE:
                command.del(getRedisKey());
                break;
        }
    }
}
