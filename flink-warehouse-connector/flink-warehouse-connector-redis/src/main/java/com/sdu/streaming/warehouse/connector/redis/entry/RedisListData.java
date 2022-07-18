package com.sdu.streaming.warehouse.connector.redis.entry;

import com.sdu.streaming.warehouse.connector.redis.AbstractRedisData;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.types.RowKind;

import java.util.LinkedList;
import java.util.List;

public class RedisListData extends AbstractRedisData<byte[][]> {


    public RedisListData(long expireTime, RowKind kind, byte[] keys, byte[][] values) {
        super(expireTime, kind, keys, values);
    }


    @Override
    public List<RedisFuture<?>> save(StatefulRedisConnection<byte[], byte[]> client) {
        RedisAsyncCommands<byte[], byte[]> command = client.async();
        List<RedisFuture<?>> result = new LinkedList<>();

        switch (getRedisDataKind()) {
            case INSERT:
            case UPDATE_AFTER:
                // delete old, append new
                result.add(command.del(getRedisKey()));
                result.add(command.rpush(getRedisKey(), getRedisValue()));
                result.add(command.expire(getRedisKey(), expireTime()));
                break;

            case DELETE:
            case UPDATE_BEFORE:
                result.add(command.del(getRedisKey()));
                break;
        }

        return result;
    }
}
