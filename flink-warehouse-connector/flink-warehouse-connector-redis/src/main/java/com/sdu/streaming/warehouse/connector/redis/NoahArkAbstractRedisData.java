package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisData;
import org.apache.flink.types.RowKind;

public abstract class NoahArkAbstractRedisData<T> implements NoahArkRedisData<T> {

    private final long expireTime;
    private final RowKind kind;
    private final byte[] keys;
    private final T values;

    public NoahArkAbstractRedisData(long expireTime, RowKind kind, byte[] keys, T values) {
        this.expireTime = expireTime;
        this.kind = kind;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public long expireTime() {
        return expireTime;
    }

    public RowKind getRedisDataKind() {
        return kind;
    }

    @Override
    public byte[] getRedisKey() {
        return keys;
    }

    @Override
    public T getRedisValue() {
        return values;
    }
}
