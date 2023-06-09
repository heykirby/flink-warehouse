package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
public class TaskLineage implements Serializable {

    @JsonProperty("task_name")
    private final String taskName;

    @JsonProperty("update_time")
    private final String updateTime;

    @JsonProperty("info")
    private final List<Lineage> lineages;

    public TaskLineage(String taskName, List<Lineage> lineages) {
        this.taskName = taskName;
        this.lineages = lineages;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.updateTime = df.format(new Date());
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public String getTaskName() {
        return taskName;
    }

    public List<Lineage> getLineages() {
        return lineages;
    }
}
