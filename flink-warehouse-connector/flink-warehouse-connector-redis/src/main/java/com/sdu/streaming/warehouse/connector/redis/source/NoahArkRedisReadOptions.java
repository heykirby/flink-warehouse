package com.sdu.streaming.warehouse.connector.redis.source;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisDataType;
import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisOptions;
import org.apache.flink.table.types.logical.RowType;


public class NoahArkRedisReadOptions extends NoahArkRedisOptions {

    private final String clusterAddress;
    private final boolean async;
    // 重试次数
    private final int maxRetryTimes;
    // 缓存
    private final boolean cacheable;
    private final long cacheMaxSize;
    private final long cacheExpireMs;

    public NoahArkRedisReadOptions(RowType rowType, String keyPrefix, NoahArkRedisDataType redisDataType, String clusterAddress, boolean async, int maxRetryTimes, boolean cacheable, long cacheMaxSize, long cacheExpireMs) {
        super(rowType, keyPrefix, redisDataType);
        this.clusterAddress = clusterAddress;
        this.async = async;
        this.maxRetryTimes = maxRetryTimes;
        this.cacheable = cacheable;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    public String getClusterAddress() {
        return clusterAddress;
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isCacheable() {
        return cacheable;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    @Override
    public long expireTime() {
        throw new UnsupportedOperationException("");
    }
}
