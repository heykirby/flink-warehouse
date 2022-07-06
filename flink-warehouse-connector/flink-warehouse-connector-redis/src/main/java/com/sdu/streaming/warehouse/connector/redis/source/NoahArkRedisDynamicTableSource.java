package com.sdu.streaming.warehouse.connector.redis.source;

import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRowDataRuntimeConverter;
import com.sdu.streaming.warehouse.connector.redis.NoahArkRedisRuntimeConverter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

public class NoahArkRedisDynamicTableSource implements LookupTableSource {

    private final TableSchema physicalSchema;
    private final NoahArkRedisReadOptions readOptions;

    public NoahArkRedisDynamicTableSource(TableSchema physicalSchema, NoahArkRedisReadOptions readOptions) {
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

        NoahArkRedisRuntimeConverter<RowData> converter = new NoahArkRedisRowDataRuntimeConverter(readOptions, primaryKeyIndexes);

        if (readOptions.isAsync()) {
            throw new UnsupportedOperationException("waiting develop");
//            return AsyncTableFunctionProvider.of(null);
        }
        return TableFunctionProvider.of(new NoahArkRedisTableFunction(converter, readOptions));
    }

    @Override
    public DynamicTableSource copy() {
        return new NoahArkRedisDynamicTableSource(physicalSchema, readOptions);
    }

    @Override
    public String asSummaryString() {
        return "Kwai Redis";
    }

}
