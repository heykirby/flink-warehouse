package com.sdu.streaming.warehouse.connector.redis;

import java.util.Map;

import org.apache.flink.types.RowKind;

public interface NoahArkRedisDataObject {

    RowKind getOperation();

    byte[] getRedisKey();

    byte[] getRedisValue();

    byte[][] getRedisValueAsList();

    Map<byte[], byte[]> getRedisValueAsMap();

}
