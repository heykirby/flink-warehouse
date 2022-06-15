package com.sdu.streaming.warehouse.entry;

import static java.lang.String.format;

import java.util.Map;
import java.util.Objects;

public class TableMetadata {

    // CREATE TABLE xxx
    // name = xxx
    private final String name;
    // 真是物理表名
    private final String physicalName;
    private final StorageType type;
    private final Map<String, String> properties;

    public TableMetadata(StorageType type, String name, String database, String table, Map<String, String> properties) {
        this(name, format("%s.%s", database, table), type, properties);
    }

    private TableMetadata(String name, String physicalName, StorageType type, Map<String, String> properties) {
        this.name = name;
        this.physicalName = physicalName;
        this.type = type;
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public String getPhysicalName() {
        return physicalName;
    }

    public String getLineageName() {
        return format("%s.%s", type.getType(), physicalName);
    }

    public StorageType getType() {
        return type;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadata metadata = (TableMetadata) o;
        return Objects.equals(name, metadata.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
