package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.Map;

public class MySQLTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final MySQLTableSqlParseHandler INSTANCE = new MySQLTableSqlParseHandler();

    private static final String PROPERTY_DATABASE = "datasource";
    private static final String PROPERTY_TABLE = "table-name";

    private MySQLTableSqlParseHandler() { }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        String datasource = properties.get(PROPERTY_DATABASE);
        String table = properties.get(PROPERTY_TABLE);
        if (datasource == null || datasource.isEmpty()) {
            throw new RuntimeException("cant find 'datasource' property for table '" + name + "'");
        }
        if (table == null || table.isEmpty()) {
            throw new RuntimeException("cant find 'table-name' property for table '" + name + "'");
        }
        return new TableMetadata(supportedType(), name, datasource, table, properties);
    }

    @Override
    public StorageType supportedType() {
        return StorageType.MYSQL;
    }
}
