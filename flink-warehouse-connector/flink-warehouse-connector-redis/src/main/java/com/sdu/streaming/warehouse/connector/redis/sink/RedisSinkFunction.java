package com.sdu.streaming.warehouse.connector.redis.sink;

import com.sdu.streaming.warehouse.connector.redis.RedisRuntimeConverter;
import com.sdu.streaming.warehouse.connector.redis.entry.RedisData;
import com.sdu.streaming.warehouse.utils.MoreFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class RedisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSinkFunction.class);

    private final RedisWriteOptions writeOptions;
    private final RedisRuntimeConverter<T> converter;

    // 非集群模式
    private transient RedisClient client;
    private transient StatefulRedisConnection<byte[], byte[]> connection;

    private transient RedisBufferQueue<RedisData<?>> bufferQueue;

    // async write
    private transient AtomicInteger batchCount = new AtomicInteger(0);
    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;

    private transient volatile boolean closed = false;

    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public RedisSinkFunction(RedisWriteOptions writeOptions, RedisRuntimeConverter<T> converter) {
        this.writeOptions = writeOptions;
        this.converter = converter;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        LOG.info("task[{} / {}] start initialize redis connection",
                getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
        converter.open();
        bufferQueue = new RedisSyncBufferQueue<>();
        if (writeOptions.getBufferFlushInterval() != 0 && writeOptions.getBufferFlushMaxSize() != 1) {
            executor = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("redis-sink-flusher"));
            scheduledFuture = executor.scheduleWithFixedDelay(
                    () -> {
                        if (!closed) {
                            try {
                                flush();
                            } catch (Exception e) {
                                // fail the sink and skip the rest of the items
                                // if the failure handler decides to throw an exception
                                failureThrowable.compareAndSet(null, e);
                            }
                        }
                    },
                    writeOptions.getBufferFlushInterval(),
                    writeOptions.getBufferFlushInterval(),
                    TimeUnit.SECONDS
            );
        }
        client = RedisClient.create(writeOptions.getClusterName());
        connection = client.connect(new ByteArrayCodec());
        connection.setAutoFlushCommands(false);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // flush buffer
        if (batchCount.get() != 0) {
            flush();
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErrorAndRethrow();
        bufferQueue.buffer(converter.serialize(value));
        if (batchCount.incrementAndGet() >= writeOptions.getBufferFlushMaxSize()) {
            flush();
        }
    }

    private void flush() {
        try {
            bufferQueue.flush(this::doFlush);
            batchCount.set(0);
        } catch (Exception e) {
            failureThrowable.compareAndSet(null, e);
        }
        checkErrorAndRethrow();
    }

    private void doFlush(List<RedisData<?>> bufferData) {
        // AsyncCommand + FlushCommands --> Redis Pipeline
        final List<RedisFuture<?>> result = new LinkedList<>();
        bufferData.forEach(redisData -> result.addAll(redisData.save(connection)));
        connection.flushCommands();
        MoreFutures.tryAwait(result);
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("an error occurred in RedisSink.", cause);
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (connection != null) {
            connection.close();
            client.shutdown();
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }
}