package com.sdu.streaming.frog.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class FrogJobConfiguration implements Serializable {

    // 是否流模式
    private boolean streaming;
    // 快照配置
    private FogJobCheckpointConfiguration checkpointCfg;
    // 运行配置
    private Map<String, Object> options;

    @Data
    public static class FogJobCheckpointConfiguration implements Serializable {

        private long checkpointTimeout;
        private long checkpointInterval;
        private int tolerableCheckpointFailureNumber;

        private boolean exactlyOnce;
        private boolean approximateLocalRecovery;
        private boolean unalignedCheckpointsEnabled;

    }

    public static Map<String, Object> getDefaultConfiguration() {
        Map<String, Object> defaultOptions = new HashMap<>();
        // checkpoint configuration, @See ExecutionCheckpointingOptions
        defaultOptions.put("execution.checkpointing.mode", "EXACTLY_ONCE");
        defaultOptions.put("execution.checkpointing.timeout", 10 * 60 * 1000L);
        defaultOptions.put("execution.checkpointing.interval", 15 * 60 * 1000L);
        defaultOptions.put("execution.checkpointing.tolerable-failed-checkpoints", 10);
        return defaultOptions;
    }
}
