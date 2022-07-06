package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.types.RowKind;

public class NoahArkRedisStringObject extends NoahArkAbstractRedisObject {

    private final byte[] values;

    public NoahArkRedisStringObject(RowKind kind, byte[] keys, byte[] values) {
        super(kind, keys);
        this.values = values;
    }

    @Override
    public byte[] getRedisValue() {
        return values;
    }
}
