package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.Map;

import static com.sdu.streaming.warehouse.entry.StorageType.REDIS;

public class RedisTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final RedisTableSqlParseHandler INSTANCE = new RedisTableSqlParseHandler();

    private static final String PROPERTY_DATABASE = "redis-address";
    private static final String PROPERTY_TABLE = "redis-key-prefix";

    private RedisTableSqlParseHandler() { }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        String datasource = properties.get(PROPERTY_DATABASE);
        String table = properties.get(PROPERTY_TABLE);
        if (datasource == null || datasource.isEmpty()) {
            throw new RuntimeException("cant find 'redis-address' property for table '" + name + "'");
        }
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("cant find 'redis-key-prefix' property for table '" + name + "'");
        }
        return new TableMetadata(supportedType(), name, datasource, table, properties);
    }

    @Override
    public StorageType supportedType() {
        return REDIS;
    }
}
