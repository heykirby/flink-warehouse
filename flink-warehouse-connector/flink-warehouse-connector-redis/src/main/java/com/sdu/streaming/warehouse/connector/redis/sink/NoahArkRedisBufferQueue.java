package com.sdu.streaming.warehouse.connector.redis.sink;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

public interface NoahArkRedisBufferQueue<T> extends Serializable {

    long bufferSize();

    void buffer(T data);

    void flush(Consumer<List<T>> flusher);

}
