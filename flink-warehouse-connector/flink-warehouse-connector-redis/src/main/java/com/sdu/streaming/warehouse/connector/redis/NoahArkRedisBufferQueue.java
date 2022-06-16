package com.sdu.streaming.warehouse.connector.redis;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

// TODO: 若存储慢有内存爆满风险
// TODO: Disruptor 框架
public class NoahArkRedisBufferQueue<T> implements Serializable {

    private volatile boolean flushing;

    // 单线程访问
    private List<T> currentBuffer;
    // 确保某个时刻只有单个线程访问
    private final List<List<T>> waitingFlushBuffers;
    // 多个线程并发访问
    private final AtomicLong bufferElementSize;


    public NoahArkRedisBufferQueue() {
        this.flushing = false;
        currentBuffer = new LinkedList<>();
        waitingFlushBuffers = new LinkedList<>();
        bufferElementSize = new AtomicLong(0);
    }

    public long bufferSize() {
        return bufferElementSize.get();
    }

    public void buffer(T data) {
        currentBuffer.add(data);
        bufferElementSize.incrementAndGet();
    }

    public void flush(Consumer<List<T>> flushFunction) {
        if (flushing) {
            return;
        }
        synchronized (this) {
            if (flushing) {
                return;
            }
            this.flushing = true;
            waitingFlushBuffers.add(currentBuffer);
            currentBuffer = new LinkedList<>();
            Iterator<List<T>> iterator = waitingFlushBuffers.iterator();
            while (iterator.hasNext()) {
                List<T> buffer = iterator.next();
                int size = buffer.size();
                flushFunction.accept(buffer);
                bufferElementSize.getAndAdd(-size);
                iterator.remove();
            }
            flushing = false;
        }
    }
}
