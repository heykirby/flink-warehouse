package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class WarehouseLineage implements Serializable {

    private final String jobName;

    private Set<String> sourceTableNames;
    // key: fullTableName, value: sourceTableOptions
    private Map<String, Map<String, String>> sourceTableOptions;

    private String sinkTableName;
    private Map<String, String> sinkTableOptions;

    // key: targetTableColumn, value: sourceTableColumn
    private Map<String, Set<String>> columnLineages;

    public WarehouseLineage(String jobName) {
        this.jobName = jobName;
    }

    public void buildTableLineage(
            String sinkTableName,
            Map<String, String> sinkTableOptions,
            Set<String> sourceTableNames,
            Map<String, Map<String, String>> sourceTableOptions) {
        checkArgument(sinkTableName != null && !sinkTableName.isEmpty());
        checkArgument(sinkTableOptions != null && sinkTableOptions.size() > 0);
        checkArgument(sourceTableNames != null && sourceTableNames.size() > 0);
        checkArgument(sourceTableOptions != null && !sourceTableOptions.isEmpty());

        this.sourceTableNames = sourceTableNames;
        this.sourceTableOptions = sourceTableOptions;
        this.sinkTableName = sinkTableName;
        this.sinkTableOptions = sinkTableOptions;
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

    public Set<String> getSourceTableNames() {
        return sourceTableNames;
    }

    public Map<String, Map<String, String>> getSourceTableOptions() {
        return sourceTableOptions;
    }

    public String getSinkTableName() {
        return sinkTableName;
    }

    public Map<String, String> getSinkTableOptions() {
        return sinkTableOptions;
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
