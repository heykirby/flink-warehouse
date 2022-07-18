package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.entry.RedisDataType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public abstract class RedisOptions implements Serializable {

    private final RowType rowType;
    private final String keyPrefix;
    private final RedisDataType redisDataType;

    public RedisOptions(RowType rowType, String keyPrefix, RedisDataType redisDataType) {
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

    public RedisDataType getRedisDataType() {
        return redisDataType;
    }

    public abstract long expireTime();
}
