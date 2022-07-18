package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.Map;

import static com.sdu.streaming.warehouse.entry.StorageType.REDIS;

public class RedisTableSqlParseHandler extends AbstractSqlParseHandler {

    private static final String PROPERTY_DATABASE = "";
    private static final String PROPERTY_TABLE = "";

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        return null;
    }

    @Override
    public StorageType supportedType() {
        return REDIS;
    }
}
