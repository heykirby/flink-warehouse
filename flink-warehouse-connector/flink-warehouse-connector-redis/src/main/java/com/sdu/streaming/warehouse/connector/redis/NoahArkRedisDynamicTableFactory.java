package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.sdu.streaming.warehouse.connector.redis.NoahArkRedisConfigOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class NoahArkRedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return null;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        NoahArkRedisWriteOptions writeOptions = getRedisWriteOptions(helper.getOptions(), context);

        return new NoahArkRedisDynamicTableSink(writeOptions);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_CLUSTER);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_STORAGE_STRUCTURE);
        options.add(REDIS_KEY_PREFIX);
        options.add(REDIS_KEY_SEPARATOR);

        // string
        options.add(REDIS_VALUE_SEPARATOR);

        // write
        options.add(REDIS_EXPIRE_SECONDS);
        options.add(REDIS_WRITE_BATCH_SIZE);
        options.add(REDIS_WRITE_FLUSH_INTERVAL);
        options.add(REDIS_WRITE_PARALLELISM);

        return options;
    }

    private static NoahArkRedisWriteOptions getRedisWriteOptions(ReadableConfig tableOption, Context context) {
        DataType rowDataType =  context.getCatalogTable().getSchema().toPhysicalRowDataType();
        int[] primaryKeyIndexes = getPrimaryKeyIndexes(context.getCatalogTable().getResolvedSchema());

        return new NoahArkRedisWriteOptions(
                (RowType) rowDataType.getLogicalType(),
                tableOption.get(REDIS_KEY_PREFIX),
                tableOption.get(REDIS_KEY_SEPARATOR),
                primaryKeyIndexes,
                tableOption.get(REDIS_VALUE_SEPARATOR),
                tableOption.get(REDIS_CLUSTER),
                tableOption.get(REDIS_WRITE_BATCH_SIZE),
                tableOption.get(REDIS_WRITE_FLUSH_INTERVAL),
                tableOption.get(REDIS_EXPIRE_SECONDS),
                tableOption.get(REDIS_STORAGE_STRUCTURE),
                tableOption.get(REDIS_WRITE_PARALLELISM)
        );
    }

    private static int[] getPrimaryKeyIndexes(ResolvedSchema schema) {
        final List<String> columns = schema.getColumnNames();
        return schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .map(pkColumns -> pkColumns.stream().mapToInt(columns::indexOf).toArray())
                .orElseGet(() -> new int[] {});
    }
}
