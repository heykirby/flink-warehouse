package com.sdu.streaming.warehouse.connector.redis.sink;

import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRowDataRuntimeConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class NoahArkRedisDynamicTableSink implements DynamicTableSink {

    private final int[][] primaryKeyIndexes;
    private final NoahArkRedisWriteOptions writeOptions;

    public NoahArkRedisDynamicTableSink(NoahArkRedisWriteOptions writeOptions, int[] primaryKeyIndexes) {
        this.writeOptions = writeOptions;
        this.primaryKeyIndexes = new int[primaryKeyIndexes.length][2];
        for (int i = 0; i < primaryKeyIndexes.length; ++i) {
            this.primaryKeyIndexes[i] = new int[] {primaryKeyIndexes[i], primaryKeyIndexes[i]};
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        NoahArkRedisSinkFunction<RowData> sinkFunction = new NoahArkRedisSinkFunction<>(writeOptions,
                new NoahArkRedisRowDataRuntimeConverter(writeOptions, primaryKeyIndexes));
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
        int[] keyIndexes = new int[primaryKeyIndexes.length];
        for (int i = 0; i < primaryKeyIndexes.length; ++i) {
            keyIndexes[i] = primaryKeyIndexes[i][0];
        }
        return new NoahArkRedisDynamicTableSink(writeOptions, keyIndexes);
    }

    @Override
    public String asSummaryString() {
        return "Redis";
    }
}
