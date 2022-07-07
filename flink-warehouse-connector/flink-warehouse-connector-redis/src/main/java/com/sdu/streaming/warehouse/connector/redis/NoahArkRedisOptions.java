package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisDataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public abstract class NoahArkRedisOptions implements Serializable {

    private final RowType rowType;
    private final String keyPrefix;
    private final NoahArkRedisDataType redisDataType;

    public NoahArkRedisOptions(RowType rowType, String keyPrefix, NoahArkRedisDataType redisDataType) {
        this.rowType = rowType;
        this.keyPrefix = keyPrefix;
        this.redisDataType = redisDataType;
    }

    public RowType getRowType() {
        return rowType;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public NoahArkRedisDataType getRedisDataType() {
        return redisDataType;
    }

    public abstract long expireTime();
}
