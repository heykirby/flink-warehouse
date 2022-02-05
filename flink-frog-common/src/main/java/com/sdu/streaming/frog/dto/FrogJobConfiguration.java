package com.sdu.streaming.frog.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class FrogJobConfiguration implements Serializable {

    public static FogJobCheckpointConfiguration DEFAULT_CK_CFG;

    static {
        DEFAULT_CK_CFG = new FogJobCheckpointConfiguration();
        DEFAULT_CK_CFG.setCheckpointTimeout(10 * 60 * 1000L);
        DEFAULT_CK_CFG.setCheckpointInterval(15 * 60 * 1000L);
        DEFAULT_CK_CFG.setCheckpointInterval(10);
        DEFAULT_CK_CFG.setExactlyOnce(true);
        DEFAULT_CK_CFG.setUnalignedCheckpointsEnabled(false);
        DEFAULT_CK_CFG.setApproximateLocalRecovery(false);
    }

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
}
