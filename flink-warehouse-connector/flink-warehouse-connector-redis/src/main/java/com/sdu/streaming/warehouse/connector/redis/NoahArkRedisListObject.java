package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.types.RowKind;

public class NoahArkRedisListObject extends NoahArkAbstractRedisDataObject {

    private final byte[][] values;

    public NoahArkRedisListObject(RowKind kind, byte[] keys, byte[][] values) {
        super(kind, keys);
        this.values = values;
    }

    @Override
    public byte[][] getRedisValueAsList() {
        return values;
    }
}
