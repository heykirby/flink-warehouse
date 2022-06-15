package com.sdu.streaming.warehouse.connector.redis;

import java.io.IOException;
import java.io.Serializable;

import org.apache.flink.configuration.Configuration;

public interface NoahArkRedisDataObjectConverter<T> extends Serializable {

    void open(Configuration cfg);

    NoahArkRedisDataObject serialize(T data) throws IOException;

}
