package com.sdu.streaming.warehouse.connector.redis.source;

import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRuntimeConverter;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.apache.flink.shaded.curator5.com.google.common.cache.Cache;
import org.apache.flink.shaded.curator5.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class NoahArkRedisAsyncTableFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(NoahArkRedisAsyncTableFunction.class);

    private final NoahArkRedisReadOptions readOptions;
    private final NoahArkRedisRuntimeConverter<RowData> converter;

    private transient RedisClusterClient client;
    private transient StatefulRedisClusterConnection<byte[], byte[]> connection;
    private transient Cache<RowData, RowData> cache;

    public NoahArkRedisAsyncTableFunction(NoahArkRedisReadOptions readOptions, NoahArkRedisRuntimeConverter<RowData> converter) {
        this.readOptions = readOptions;
        this.converter = converter;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        client = RedisClusterClient.create(readOptions.getClusterAddress());
        connection = client.connect(new ByteArrayCodec());
        connection.setAutoFlushCommands(false);
        if (readOptions.isCacheable() && readOptions.getCacheMaxSize() != -1 && readOptions.getCacheExpireMs() != -1) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(readOptions.getCacheExpireMs(), TimeUnit.MILLISECONDS)
                    .build();
        }
    }

    public void eval(CompletableFuture<Collection<RowData>> future, Object ... keys) {
        int currentRetry = 0;
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            RowData cacheRow = cache.getIfPresent(keyRow);
            if (cacheRow != null) {
                if (cacheRow.getArity() == 0) {
                    future.complete(Collections.emptyList());
                } else {
                    future.complete(singletonList(cacheRow));
                }
                return;
            }
        }
        fetchResult(future, currentRetry, keyRow);
    }

    private void fetchResult(CompletableFuture<Collection<RowData>> resultFuture, int currentRetry, RowData rowKey) {
        try {
            converter.asyncDeserialize(connection, rowKey, (rowData, throwable) -> {
                if (throwable != null) {
                    LOG.error("failed got data from redis, retry: {}", currentRetry, throwable);
                    if (currentRetry >= readOptions.getMaxRetryTimes()) {
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        try {
                            Thread.sleep(1000 * currentRetry);
                        } catch (InterruptedException ex) {
                            resultFuture.completeExceptionally(ex);
                        }
                        fetchResult(resultFuture, currentRetry + 1, rowKey);
                    }
                } else {
                    if (cache != null && rowData != null) {
                        cache.put(rowKey, rowData);
                        resultFuture.complete(singletonList(rowData));
                        return;
                    }
                    if (rowData != null) {
                        resultFuture.complete(singletonList(rowData));
                        return;
                    }
                    resultFuture.complete(emptyList());
                }
            });
        } catch (IOException ex) {
            LOG.error("failed got data from redis, retry: {}", currentRetry, ex);
            if (currentRetry >= readOptions.getMaxRetryTimes()) {
                resultFuture.completeExceptionally(ex);
                return;
            }
            fetchResult(resultFuture, currentRetry + 1, rowKey);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.shutdown();
        }
        if (connection != null) {
            connection.close();
        }
    }

}
