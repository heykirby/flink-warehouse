package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.Map;

@Deprecated
public class ConsoleTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final ConsoleTableSqlParseHandler INSTANCE = new ConsoleTableSqlParseHandler();

    private ConsoleTableSqlParseHandler() { }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        return new TableMetadata(supportedType(), name, "console", name, properties);
    }

    @Override
    public StorageType supportedType() {
        return StorageType.CONSOLE;
    }

}
