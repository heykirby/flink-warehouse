package com.sdu.streaming.warehouse.connector.redis.sink;

import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRowDataRuntimeConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class NoahArkRedisDynamicTableSink implements DynamicTableSink {

    private final NoahArkRedisWriteOptions writeOptions;

    public NoahArkRedisDynamicTableSink(NoahArkRedisWriteOptions writeOptions) {
        this.writeOptions = writeOptions;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        NoahArkRedisSinkFunction<RowData> sinkFunction = new NoahArkRedisSinkFunction<>(writeOptions,
                new NoahArkRedisRowDataRuntimeConverter(writeOptions));
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
        return new NoahArkRedisDynamicTableSink(writeOptions);
    }

    @Override
    public String asSummaryString() {
        return "Kwai Redis";
    }
}
