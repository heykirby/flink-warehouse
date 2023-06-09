package com.sdu.streaming.warehouse.entry;

import java.io.Serializable;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
public class Lineage implements Serializable {

    @JsonProperty("task_type")
    private final String taskType;

    @JsonProperty("upstream_source")
    private final String upstreamSource;

    @JsonProperty("downstream_source")
    private final String downstreamSource;

    public Lineage(TaskType type, TableMetadata source, TableMetadata sink) {
        this(type, source.getLineageName(), sink.getLineageName());
    }

    private Lineage(TaskType type, String inputName, String outputName) {
        this.taskType = type.name();
        this.upstreamSource = inputName;
        this.downstreamSource = outputName;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getUpstreamSource() {
        return upstreamSource;
    }

    public String getDownstreamSource() {
        return downstreamSource;
    }
}
