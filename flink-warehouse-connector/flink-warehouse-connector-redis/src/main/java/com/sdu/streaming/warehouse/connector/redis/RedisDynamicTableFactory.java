package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.connector.redis.sink.RedisDynamicTableSink;
import com.sdu.streaming.warehouse.connector.redis.sink.RedisWriteOptions;
import com.sdu.streaming.warehouse.connector.redis.source.RedisDynamicTableSource;
import com.sdu.streaming.warehouse.connector.redis.source.RedisReadOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
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

import static com.sdu.streaming.warehouse.connector.redis.RedisConfigOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final String IDENTIFIER = "redis";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        RedisReadOptions readOptions = getRedisReadOptions(helper.getOptions(), context);
        TableSchema tableSchema = context.getCatalogTable().getSchema();

        return new RedisDynamicTableSource(tableSchema, readOptions);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        RedisWriteOptions writeOptions = getRedisWriteOptions(helper.getOptions(), context);
        int[] primaryKeyIndexes = getPrimaryKeyIndexes(context.getCatalogTable().getResolvedSchema());

        return new RedisDynamicTableSink(writeOptions, primaryKeyIndexes);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_ADDRESS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(REDIS_DATA_TYPE);
        options.add(REDIS_KEY_PREFIX);
        // write
        options.add(REDIS_EXPIRE_SECONDS);
        options.add(REDIS_WRITE_BATCH_SIZE);
        options.add(REDIS_WRITE_FLUSH_INTERVAL);
        options.add(REDIS_WRITE_PARALLELISM);
        // read
        options.add(REDIS_READ_ASYNCABLE);
        options.add(REDIS_READ_RETRIES);
        options.add(REDIS_READ_CACHEABLE);
        options.add(REDIS_READ_CACHE_SIZE);
        options.add(REDIS_READ_CACHE_EXPIRE);

        return options;
    }

    private static RedisReadOptions getRedisReadOptions(ReadableConfig tableOption, Context context) {
        DataType rowDataType =  context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new RedisReadOptions(
                (RowType) rowDataType.getLogicalType(),
                tableOption.get(REDIS_KEY_PREFIX),
                tableOption.get(REDIS_DATA_TYPE),
                tableOption.get(REDIS_ADDRESS),
                tableOption.get(REDIS_READ_ASYNCABLE),
                tableOption.get(REDIS_READ_RETRIES),
                tableOption.get(REDIS_READ_CACHEABLE),
                tableOption.get(REDIS_READ_CACHE_SIZE),
                tableOption.get(REDIS_READ_CACHE_EXPIRE)
        );
    }

    private static RedisWriteOptions getRedisWriteOptions(ReadableConfig tableOption, Context context) {
        DataType rowDataType =  context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new RedisWriteOptions(
                (RowType) rowDataType.getLogicalType(),
                tableOption.get(REDIS_KEY_PREFIX),
                tableOption.get(REDIS_DATA_TYPE),
                tableOption.get(REDIS_ADDRESS),
                tableOption.get(REDIS_WRITE_BATCH_SIZE),
                tableOption.get(REDIS_WRITE_FLUSH_INTERVAL),
                tableOption.get(REDIS_EXPIRE_SECONDS),
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
