package com.sdu.streaming.warehouse.connector.redis.source;

import com.sdu.streaming.warehouse.connector.redis.RedisRowDataRuntimeConverter;
import com.sdu.streaming.warehouse.connector.redis.RedisRuntimeConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

public class RedisDynamicTableSource implements LookupTableSource {

    private final TableSchema physicalSchema;
    private final RedisReadOptions readOptions;

    public RedisDynamicTableSource(TableSchema physicalSchema, RedisReadOptions readOptions) {
        this.physicalSchema = physicalSchema;
        this.readOptions = readOptions;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // redis only supported non-nested look up keys.
        int[][] primaryKeyIndexes = new int[context.getKeys().length][2];
        for (int i = 0; i < context.getKeys().length; ++i) {
            int[] innerKeyIndexes = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyIndexes.length == 1, "redis only support non-nested look up keys");
            primaryKeyIndexes[i] = new int[] {i, innerKeyIndexes[0]};
        }

        RedisRuntimeConverter<RowData> converter = new RedisRowDataRuntimeConverter(readOptions, primaryKeyIndexes);

        if (readOptions.isAsync()) {
            return AsyncTableFunctionProvider.of(new RedisAsyncTableFunction(readOptions, converter));
        }
        return TableFunctionProvider.of(new RedisTableFunction(converter, readOptions));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(physicalSchema, readOptions);
    }

    @Override
    public String asSummaryString() {
        return "Kwai Redis";
    }

}
