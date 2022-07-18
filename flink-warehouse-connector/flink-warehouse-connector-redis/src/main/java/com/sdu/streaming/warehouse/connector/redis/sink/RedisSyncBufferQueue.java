package com.sdu.streaming.warehouse.connector.redis.sink;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class RedisSyncBufferQueue<T> implements RedisBufferQueue<T> {

    private List<T> queue;

    public RedisSyncBufferQueue() {
        this.queue = new LinkedList<>();
    }

    @Override
    public synchronized void buffer(T data) {
        queue.add(data);
    }

    @Override
    public synchronized void flush(Consumer<List<T>> flusher) {
        flusher.accept(queue);
        queue.clear();
    }
}
