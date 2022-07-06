package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class NoahArkRedisOptions implements Serializable {

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

}
