package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.Map;

import static com.sdu.streaming.warehouse.entry.StorageType.HUDI;

public class HudiTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final HudiTableSqlParseHandler INSTANCE = new HudiTableSqlParseHandler();

    private static final String PROPERTY_HUDI_DATABASE = "path";
    private static final String PROPERTY_HUDI_TABLE = "hoodie.table.name";

    private HudiTableSqlParseHandler() {

    }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        String database = properties.get(PROPERTY_HUDI_DATABASE);
        String table = properties.get(PROPERTY_HUDI_TABLE);
        if (database == null || database.isEmpty()) {
            throw new RuntimeException("cant find 'path' property for table '" + name + "'");
        }
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("cant find 'hoodie.table.name' property for table '" + name + "'");
        }
        return new TableMetadata(supportedType(), name, database, table, properties);
    }

    @Override
    public StorageType supportedType() {
        return HUDI;
    }

}
