package com.sdu.streaming.warehouse.connector.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;

public interface NoahArkRedisRuntimeConverter<T> extends Serializable {

    void open();

    NoahArkRedisObject serialize(T data) throws IOException;

    T deserialize(StatefulRedisClusterConnection<byte[], byte[]> client, RowData key) throws IOException;

}
