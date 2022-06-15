package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sdu.streaming.warehouse.entry.StorageType.GENERIC;

public class GenericTableSqlParseHandler implements SqlParseHandler {

    public static final GenericTableSqlParseHandler INSTANCE = new GenericTableSqlParseHandler();
    private static final String PROPERTY_GENERIC_DATABASE = "generic.database";
    private static final String PROPERTY_GENERIC_TABLE = "generic.table";

    private GenericTableSqlParseHandler() {

    }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        String database = properties.get(PROPERTY_GENERIC_DATABASE);
        String table = properties.get(PROPERTY_GENERIC_TABLE);
        if (database == null || database.isEmpty()) {
            throw new RuntimeException("cant find 'generic.database' property for table '" + name + "'");
        }
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("cant find 'generic.table' property for table '" + name + "'");
        }
        return new TableMetadata(supportedType(), name, database, table, properties);
    }

    @Override
    public List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink) {
        // 通用的维度表关联只允许作为源表, 暂不支持写入
        throw new UnsupportedOperationException("unsupported result write");
    }

    @Override
    public StorageType supportedType() {
        return GENERIC;
    }
}
