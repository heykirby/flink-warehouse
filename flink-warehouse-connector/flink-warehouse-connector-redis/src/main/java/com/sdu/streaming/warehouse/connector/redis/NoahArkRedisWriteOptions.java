package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class NoahArkRedisWriteOptions implements Serializable {

    private final RowType rowType;
    private final String keyPrefix;
    private final int[] primaryKeyIndexes;

    private final String clusterName;
    private final long bufferFlushMaxSize;
    private final long bufferFlushInterval;
    private final long expireSeconds;
    private final NoahArkRedisStructure structure;

    private final int parallelism;

    public NoahArkRedisWriteOptions(RowType rowType,
                                    String keyPrefix,
                                    int[] primaryKeyIndexes,
                                    String clusterName,
                                    long bufferFlushMaxSize,
                                    long bufferFlushInterval,
                                    long expireSeconds,
                                    NoahArkRedisStructure structure,
                                    int parallelism) {
        this.rowType = rowType;
        this.keyPrefix = keyPrefix;
        this.primaryKeyIndexes = primaryKeyIndexes;
        this.clusterName = clusterName;
        this.bufferFlushMaxSize = bufferFlushMaxSize;
        this.bufferFlushInterval = bufferFlushInterval;
        this.expireSeconds = expireSeconds;
        this.structure = structure;
        this.parallelism = parallelism;
    }

    public RowType getRowType() {
        return rowType;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public int[] getPrimaryKeyIndexes() {
        return primaryKeyIndexes;
    }

    public String getClusterName() {
        return clusterName;
    }

    public long getBufferFlushMaxSize() {
        return bufferFlushMaxSize;
    }

    public long getBufferFlushInterval() {
        return bufferFlushInterval;
    }

    public long getExpireSeconds() {
        return expireSeconds;
    }

    public NoahArkRedisStructure getStructure() {
        return structure;
    }

    public int getParallelism() {
        return parallelism;
    }
}
