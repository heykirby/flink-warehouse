package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class NoahArkRedisSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(NoahArkRedisSinkFunction.class);

    private final NoahArkRedisWriteOptions writeOptions;
    private final NoahArkRedisDataObjectConverter<T> converter;

    private transient NoahArkRedisBufferQueue<NoahArkRedisDataObject> bufferQueue;
    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;

    private transient volatile boolean closed = false;

    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public NoahArkRedisSinkFunction(NoahArkRedisWriteOptions writeOptions, NoahArkRedisDataObjectConverter<T> converter) {
        this.writeOptions = writeOptions;
        this.converter = converter;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        LOG.info("task[{} / {}] start initialize redis connection",
                getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks());
        converter.open(configuration);
        bufferQueue = new NoahArkRedisBufferQueue<>();
        executor = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("redis-sink-flusher"));
        scheduledFuture = executor.scheduleWithFixedDelay(
                () -> {
                    if (closed) {
                        return;
                    }
                    try {
                        flush();
                    } catch (Exception e) {
                        // fail the sink and skip the rest of the items
                        // if the failure handler decides to throw an exception
                        failureThrowable.compareAndSet(null, e);
                    }
                },
                writeOptions.getBufferFlushInterval(),
                writeOptions.getBufferFlushInterval(),
                TimeUnit.SECONDS
        );


    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // flush buffer
        if (bufferQueue.bufferSize() != 0) {
            flush();
        }
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErrorAndRethrow();
        bufferQueue.buffer(converter.serialize(value));
        if (bufferQueue.bufferSize() >= writeOptions.getBufferFlushMaxSize()) {
            flush();
        }
    }

    private void flush() {
        try {
            bufferQueue.flush(this::doFlush);
        } catch (Exception e) {
            failureThrowable.compareAndSet(null, e);
        }
        checkErrorAndRethrow();
    }

    private void doFlush(List<NoahArkRedisDataObject> bufferData) {
        // TODO: 异步写入
        lettuce.pipeline(command -> {
            switch (writeOptions.getStructure()) {
                case MAP:
                    bufferData.forEach(kv -> {
                        RowKind kind = kv.getOperation();
                        byte[] key = kv.getRedisKey();
                        switch (kind) {
                            case INSERT:
                            case UPDATE_AFTER:
                                command.hmset(key, kv.getRedisValueAsMap());
                                command.expire(key, writeOptions.getExpireSeconds());
                                break;

                            case DELETE:
                            case UPDATE_BEFORE:
                                command.del(key);
                                break;
                        }
                    });
                    break;

                case LIST:
                    bufferData.forEach(kv -> {
                        RowKind kind = kv.getOperation();
                        byte[] key = kv.getRedisKey();
                        switch (kind) {
                            case INSERT:
                            case UPDATE_AFTER:
                                command.rpush(key, kv.getRedisValueAsList());
                                command.expire(key, writeOptions.getExpireSeconds());
                                break;

                            case DELETE:
                            case UPDATE_BEFORE:
                                command.del(key);
                                break;
                        }
                    });
                    break;

                case STRING:
                    bufferData.forEach(kv -> {
                        RowKind kind = kv.getOperation();
                        byte[] key = kv.getRedisKey();
                        switch (kind) {
                            case INSERT:
                            case UPDATE_AFTER:
                                command.set(key, kv.getRedisValue());
                                command.expire(key, writeOptions.getExpireSeconds());
                                break;

                            case DELETE:
                            case UPDATE_BEFORE:
                                command.del(key);
                                break;
                        }
                    });
                    break;

                default:
                    failureThrowable.compareAndSet(null,
                            new UnsupportedOperationException("unsupported storage structure: " + writeOptions.getStructure()));

            }
        });
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
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }


}
