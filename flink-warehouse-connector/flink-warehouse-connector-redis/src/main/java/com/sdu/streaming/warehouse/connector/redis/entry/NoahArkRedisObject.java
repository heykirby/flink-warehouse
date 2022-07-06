package com.sdu.streaming.warehouse.connector.redis.entry;

import java.util.Map;

import org.apache.flink.types.RowKind;

public interface NoahArkRedisObject {

    RowKind getOperation();

    byte[] getRedisKey();

    byte[] getRedisValue();

    byte[][] getRedisValueAsList();

    Map<byte[], byte[]> getRedisValueAsMap();

}
