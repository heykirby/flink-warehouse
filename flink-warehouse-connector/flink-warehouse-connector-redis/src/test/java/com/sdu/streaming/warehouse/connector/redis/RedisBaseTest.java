package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.api.EnvironmentSettings;
import org.junit.Before;

public abstract class RedisBaseTest {

    protected EnvironmentSettings streamSettings;

    @Before
    public void setup() {
        this.streamSettings = EnvironmentSettings.inStreamingMode();
    }

}
