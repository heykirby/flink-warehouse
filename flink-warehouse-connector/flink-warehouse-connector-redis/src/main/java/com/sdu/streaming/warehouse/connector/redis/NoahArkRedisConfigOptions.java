package com.sdu.streaming.warehouse.connector.redis;

import java.io.Serializable;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class NoahArkRedisConfigOptions implements Serializable {

    private NoahArkRedisConfigOptions() { }

    public static final ConfigOption<String> REDIS_CLUSTER =
            ConfigOptions.key("redis-cluster")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("redis cluster name");

    public static final ConfigOption<NoahArkRedisStructure> REDIS_STORAGE_STRUCTURE =
            ConfigOptions.key("redis-storage-structure")
                    .enumType(NoahArkRedisStructure.class)
                    .defaultValue(NoahArkRedisStructure.MAP)
                    .withDescription("redis data storage structure");

    public static final ConfigOption<String> REDIS_KEY_PREFIX =
            ConfigOptions.key("redis-key-prefix")
                    .stringType()
                    .defaultValue("")
                    .withDescription("redis key prefix, default: empty");

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
