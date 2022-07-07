package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sdu.streaming.warehouse.entry.StorageType.REDIS;

public class RedisTableSqlParseHandler implements SqlParseHandler {

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        return null;
    }

    @Override
    public List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink) {
        return null;
    }

    @Override
    public StorageType supportedType() {
        return REDIS;
    }
}
