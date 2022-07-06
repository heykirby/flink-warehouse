package com.sdu.streaming.warehouse.connector.redis.entry;

import com.sdu.streaming.warehouse.connector.redis.NoahArkAbstractRedisObject;
import org.apache.flink.types.RowKind;

public class NoahArkRedisListObject extends NoahArkAbstractRedisObject {

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
