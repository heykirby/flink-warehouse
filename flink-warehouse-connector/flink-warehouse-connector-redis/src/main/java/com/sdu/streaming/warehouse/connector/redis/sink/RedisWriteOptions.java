package com.sdu.streaming.warehouse.connector.redis.sink;

import com.sdu.streaming.warehouse.connector.redis.RedisOptions;
import com.sdu.streaming.warehouse.connector.redis.entry.RedisDataType;
import org.apache.flink.table.types.logical.RowType;

public class RedisWriteOptions extends RedisOptions {

    private final String clusterName;
    private final int bufferFlushMaxSize;
    private final int bufferFlushInterval;
    private final long expireSeconds;
    private final int parallelism;

    public RedisWriteOptions(RowType rowType,
                             String keyPrefix,
                             RedisDataType redisDataType,
                             String clusterName,
                             int bufferFlushMaxSize,
                             int bufferFlushInterval,
                             long expireSeconds,
                             int parallelism) {
        super(rowType, keyPrefix, redisDataType);
        this.clusterName = clusterName;
        this.bufferFlushMaxSize = bufferFlushMaxSize;
        this.bufferFlushInterval = bufferFlushInterval;
        this.expireSeconds = expireSeconds;
        this.parallelism = parallelism;
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getBufferFlushMaxSize() {
        return bufferFlushMaxSize;
    }

    public int getBufferFlushInterval() {
        return bufferFlushInterval;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public long expireTime() {
        return expireSeconds;
    }
}
