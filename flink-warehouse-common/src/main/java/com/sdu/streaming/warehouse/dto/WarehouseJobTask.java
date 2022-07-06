package com.sdu.streaming.warehouse.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class WarehouseJobTask implements Serializable {
    // 任务名
    private String name;
    // 是否流模式
    private boolean streaming;
    // 是否上报血缘
    private boolean reportLineage;
    /*
     * supported sql statement:
     * 1. SET 'key' = 'value'
     * */
    private List<String> configurations;
    /*
     * supported sql statement:
     * 1. CREATE TABLE ...
     * 2. CREATE FUNCTION ...
     * 3. CREATE VIEW
     * */
    private List<String> materials;
    /*
     * supported sql statement:
     * 1. INSERT INTO ...
     * */
    private List<String> calculates;
}
