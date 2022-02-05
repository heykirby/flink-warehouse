package com.sdu.streaming.frog.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class FrogJobConfiguration implements Serializable {
    // 是否流模式
    private boolean streaming;
    // 快照配置
    private FogJobCheckpointConfiguration checkpointCfg;
    // 运行配置
    private Map<String, Object> options;

    public static class FogJobCheckpointConfiguration implements Serializable {

    }
}
