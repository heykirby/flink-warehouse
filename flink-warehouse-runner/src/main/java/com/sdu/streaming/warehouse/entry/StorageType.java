package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;
import java.util.Map;

public enum StorageType implements Serializable {

    KAFKA(0, "kafka"),
    ELASTICSEARCH(1, "elasticsearch"),
    CLICKHOUSE(2, "clickhouse"),
    DRUID(3, "druid"),
    MYSQL(4, "mysql"),
    REDIS(5, "redis"),
    HUDI(6, "hudi"),
    GENERIC(7, "generic");


    private final int code;

    private final String type;

    StorageType(int code, String type) {
        this.code = code;
        this.type = type;
    }

    public int getCode() {
        return code;
    }

    public String getType() {
        return type;
    }


    public static StorageType fromProperties(Map<String, String> properties) {
        String connector = properties.get("connector");
        if (connector == null || connector.isEmpty()) {
            throw new RuntimeException("cant find 'connector' property");
        }
        switch (connector) {
            case "ks_kafka":
            case "ks_upsert-kafka":
                return KAFKA;
            case "ks_kdb":
                return MYSQL;
            case "hudi":
                return HUDI;
            case "generic-table":
                // 维表
                return GENERIC;
            default:
                throw new UnsupportedOperationException("unsupported connector: " + connector);
        }
    }

    public static StorageType fromName(String name) {
        StorageType[] types = StorageType.values();
        for (StorageType type : types) {
            if (type.getType().equalsIgnoreCase(name)) {
                return type;
            }
        }
        throw new UnsupportedOperationException("cant find storage type for name '" + name + "'");
    }
}
