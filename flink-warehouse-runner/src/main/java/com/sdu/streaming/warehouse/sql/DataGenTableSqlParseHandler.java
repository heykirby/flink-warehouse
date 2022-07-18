package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataGenTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final DataGenTableSqlParseHandler INSTANCE = new DataGenTableSqlParseHandler();

    private DataGenTableSqlParseHandler() { }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        return new TableMetadata(supportedType(), name, "mock", name, properties);
    }

    @Override
    public List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink) {
        throw new UnsupportedOperationException("not supported as an sink table.");
    }

    @Override
    public StorageType supportedType() {
        return StorageType.DATAGEN;
    }

}
