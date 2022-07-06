package com.sdu.streaming.warehouse.connector.redis;

import java.util.Map;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisObject;
import org.apache.flink.types.RowKind;

public abstract class NoahArkAbstractRedisObject implements NoahArkRedisObject {

    private final RowKind kind;
    private final byte[] keys;

    public NoahArkAbstractRedisObject(RowKind kind, byte[] keys) {
        this.kind = kind;
        this.keys = keys;
    }

    @Override
    public RowKind getOperation() {
        return kind;
    }

    @Override
    public byte[] getRedisKey() {
        return keys;
    }

    @Override
    public byte[] getRedisValue() {
        throw new UnsupportedOperationException("unsupported bytes type.");
    }

    @Override
    public byte[][] getRedisValueAsList() {
        throw new UnsupportedOperationException("unsupported map type.");
    }

    @Override
    public Map<byte[], byte[]> getRedisValueAsMap() {
        throw new UnsupportedOperationException("unsupported list type.");
    }
}
