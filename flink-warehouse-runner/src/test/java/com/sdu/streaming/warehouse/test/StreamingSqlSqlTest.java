package com.sdu.streaming.warehouse.test;

import com.google.common.collect.Lists;
import com.sdu.streaming.warehouse.WarehouseJobBootstrap;
import com.sdu.streaming.warehouse.dto.WarehouseJobTask;
import com.sdu.streaming.warehouse.utils.Base64Utils;
import com.sdu.streaming.warehouse.utils.JsonUtils;
import org.junit.Before;

import static java.lang.String.format;

public class StreamingSqlSqlTest {

    protected WarehouseJobTask task;

    @Before
    public void setup() {
        task = new WarehouseJobTask();
        task.setName("streaming-warehouse-sql-task");
        task.setMaterials(
                Lists.newArrayList(
                        "CREATE TABLE t1 (pid BIGINT, name STRING, PRICE DOUBLE, ptime BIGINT, ts AS TO_TIMESTAMP_LTZ(ptime, 3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) WITH ('connector' = 'datagen')"
                )
        );
    }

    protected void execute() throws Exception {
        String taskJson = JsonUtils.toJson(task);
        String encodeTaskConfig = Base64Utils.encode(taskJson);
        String[] args = new String[] {format("--taskConfig %s", encodeTaskConfig)};
        WarehouseJobBootstrap.run(args);
    }

}
