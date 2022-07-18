package com.sdu.streaming.warehouse.connector.redis.sink;

import com.sdu.streaming.warehouse.connector.redis.RedisRowDataRuntimeConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class RedisDynamicTableSink implements DynamicTableSink {

    private final int[] primaryKeyIndexes;
    private final RedisWriteOptions writeOptions;

    public RedisDynamicTableSink(RedisWriteOptions writeOptions, int[] primaryKeyIndexes) {
        this.writeOptions = writeOptions;
        this.primaryKeyIndexes = primaryKeyIndexes;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        int[][] keyIndexes = new int[primaryKeyIndexes.length][2];
        for (int i = 0; i < primaryKeyIndexes.length; ++i) {
            keyIndexes[i] = new int[] {primaryKeyIndexes[i], primaryKeyIndexes[i]};
        }
        RedisSinkFunction<RowData> sinkFunction = new RedisSinkFunction<>(writeOptions,
                new RedisRowDataRuntimeConverter(writeOptions, keyIndexes));
        return SinkFunctionProvider.of(sinkFunction, writeOptions.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(writeOptions, primaryKeyIndexes);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
