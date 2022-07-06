package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class NoahArkRedisOptions implements Serializable {

    private final RowType rowType;
    private final int[] primaryKeyIndexes;
    private final String keyPrefix;
    private final NoahArkRedisDataType redisDataType;

    public NoahArkRedisOptions(RowType rowType, int[] primaryKeyIndexes, String keyPrefix, NoahArkRedisDataType redisDataType) {
        this.rowType = rowType;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.keyPrefix = keyPrefix;
        this.redisDataType = redisDataType;
    }

    public RowType getRowType() {
        return rowType;
    }

    public int[] getPrimaryKeyIndexes() {
        return primaryKeyIndexes;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public NoahArkRedisDataType getRedisDataType() {
        return redisDataType;
    }

}
