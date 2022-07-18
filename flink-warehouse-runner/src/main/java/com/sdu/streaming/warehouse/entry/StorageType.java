package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;
import java.util.Map;

public enum StorageType implements Serializable {

    KAFKA(0, "kafka"),
    DATAGEN(1, "datagen"),
    CONSOLE(2, "console"),
    REDIS(3, "redis");


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
            case "zh_kafka":
                return KAFKA;
            case "datagen":
                return DATAGEN;
            case "print":
                return CONSOLE;
            case "redis":
                return REDIS;
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
