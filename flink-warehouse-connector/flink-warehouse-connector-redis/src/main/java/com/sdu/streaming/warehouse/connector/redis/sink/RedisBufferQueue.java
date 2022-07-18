package com.sdu.streaming.warehouse.connector.redis.sink;

import java.io.Serializable;
import java.util.List;
import java.util.function.Consumer;

public interface RedisBufferQueue<T> extends Serializable {

    void buffer(T data);

    void flush(Consumer<List<T>> flusher);

}
