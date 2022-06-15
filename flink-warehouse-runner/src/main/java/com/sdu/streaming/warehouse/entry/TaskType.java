package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;

public enum TaskType implements Serializable {

    COMPUTE("compute", "process buried point data"),
    SYNC("sync", "sync data to clickhouse");

    private final String type;
    private final String description;

    TaskType(String type, String description) {
        this.type = type;
        this.description = description;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }
}
