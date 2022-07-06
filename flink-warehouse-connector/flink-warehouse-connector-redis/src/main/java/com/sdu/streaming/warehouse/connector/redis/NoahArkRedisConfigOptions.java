package com.sdu.streaming.warehouse.connector.redis;

import java.io.Serializable;

import com.sdu.streaming.warehouse.connector.redis.entry.NoahArkRedisDataType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class NoahArkRedisConfigOptions implements Serializable {

    private NoahArkRedisConfigOptions() { }

    public static final ConfigOption<String> REDIS_CLUSTER =
            ConfigOptions.key("redis-cluster")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis cluster name");

    public static final ConfigOption<NoahArkRedisDataType> REDIS_STORAGE_TYPE =
            ConfigOptions.key("redis-storage-type")
                    .enumType(NoahArkRedisDataType.class)
                    .defaultValue(NoahArkRedisDataType.STRING)
                    .withDescription("redis data storage type, default: string");

    public static final ConfigOption<String> REDIS_KEY_PREFIX =
            ConfigOptions.key("redis-key-prefix")
                    .stringType()
                    .defaultValue("")
                    .withDescription("redis key prefix, default: empty");


    // Read
    public static final ConfigOption<Boolean> REDIS_READ_ASYNCABLE =
            ConfigOptions.key("redis-read-asyncable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("support async lookup join redis, default: false.");

    public static final ConfigOption<Integer> REDIS_READ_RETRIES =
            ConfigOptions.key("redis-read-retries")
                    .intType()
                    .defaultValue(2)
                    .withDescription("retry read when failed, default: 2");

    public static final ConfigOption<Boolean> REDIS_READ_CACHEABLE =
            ConfigOptions.key("redis-read-cacheable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("support redis data cacheable, default: true");

    public static final ConfigOption<Integer> REDIS_READ_CACHE_SIZE =
            ConfigOptions.key("redis-read-cache-size")
                    .intType()
                    .defaultValue(64)
                    .withDescription("redis data cache size, default: 64");

    public static final ConfigOption<Long> REDIS_READ_CACHE_EXPIRE =
            ConfigOptions.key("redis-read-cache-expire")
                    .longType()
                    .defaultValue(10 * 60 * 1000L)
                    .withDescription("redis cache expire time, default: 10 * 60 * 1000L ms");

    // Write
    public static final ConfigOption<Integer> REDIS_WRITE_BATCH_SIZE =
            ConfigOptions.key("redis-write-batch-size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("write batch write max size, default: 100");

    public static final ConfigOption<Integer> REDIS_WRITE_FLUSH_INTERVAL =
            ConfigOptions.key("redis-write-flush-interval")
                    .intType()
                    .defaultValue(60)
                    .withDescription("write buffer flush interval, default: 60s");

    public static final ConfigOption<Long> REDIS_EXPIRE_SECONDS =
            ConfigOptions.key("redis-expire-seconds")
                    .longType()
                    .defaultValue(24 * 60 * 60L)
                    .withDescription("redis expire seconds, default: 24 * 60 * 60L");

    public static final ConfigOption<Integer> REDIS_WRITE_PARALLELISM =
            ConfigOptions.key("redis-write-parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("write parallelism, default: 1");


}
