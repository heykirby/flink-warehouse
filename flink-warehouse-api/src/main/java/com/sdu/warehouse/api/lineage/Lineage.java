package com.sdu.warehouse.api.lineage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Lineage implements Serializable {

    private final String jobName;

    // key: sourceFullTableName, value: sourceTableOptions
    private Map<String, Map<String, String>> sourceFullTableNames;

    private String targetFullTableName;
    private Map<String, String> targetTableOptions;

    // key: targetTableColumn, value: sourceTableColumn
    private Map<String, Set<String>> columnLineages;

    public Lineage(String jobName) {
        this.jobName = jobName;
    }

    public void buildTableLineage(Map<String, Map<String, String>> sourceFullTableNames,
                                  String targetFullTableName,
                                  Map<String, String> targetTableOptions) {
        checkArgument(sourceFullTableNames != null && !sourceFullTableNames.isEmpty());
        checkArgument(targetFullTableName != null && !targetFullTableName.isEmpty());
        checkArgument(targetTableOptions != null);

        this.sourceFullTableNames = sourceFullTableNames;
        this.targetFullTableName = targetFullTableName;
        this.targetTableOptions = targetTableOptions;
    }

    public void addTableColumnLineage(String sourceColumnName, String targetColumnName) {
        checkArgument(sourceColumnName != null && !sourceColumnName.isEmpty());
        checkArgument(targetColumnName != null && !targetColumnName.isEmpty());

        if (columnLineages == null) {
            columnLineages = new HashMap<>();
        }
        Set<String> sourceColumns = columnLineages.computeIfAbsent(targetColumnName, k -> new HashSet<>());
        sourceColumns.add(sourceColumnName);
    }

    public String getJobName() {
        return jobName;
    }

    public Map<String, Map<String, String>> getSourceFullTableNames() {
        return sourceFullTableNames;
    }

    public String getTargetFullTableName() {
        return targetFullTableName;
    }

    public Map<String, String> getTargetTableOptions() {
        return targetTableOptions;
    }

    public Map<String, Set<String>> getColumnLineages() {
        return columnLineages;
    }

    private static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }
}
