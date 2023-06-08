package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.WarehouseJobBootstrap;
import com.sdu.streaming.warehouse.dto.TableColumnMetadata;
import com.sdu.streaming.warehouse.dto.TableMetadata;
import com.sdu.streaming.warehouse.dto.TableWatermarkMetadata;
import com.sdu.streaming.warehouse.dto.WarehouseJob;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static com.sdu.streaming.warehouse.utils.Base64Utils.encode;
import static com.sdu.streaming.warehouse.utils.JsonUtils.toJson;

public class StreamingSqlBaseTest {

    protected WarehouseJob task;
    protected TableMetadata sourceTableMetadata;

    @Before
    public void setup() {
        // source
        Map<String, String> sourceTableProperties = Maps.newHashMap();
        sourceTableProperties.put("connector", "datagen");
        sourceTableProperties.put("number-of-rows", "10");
        sourceTableProperties.put("rows-per-second", "2");
        List<TableColumnMetadata> sourceTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("id").type("BIGINT").build(),
                TableColumnMetadata.builder().name("name").type("STRING").build(),
                TableColumnMetadata.builder().name("price").type("DOUBLE").build(),
                TableColumnMetadata.builder().name("`timestamp`").type("TIMESTAMP_LTZ(3)").build()
        );
        TableWatermarkMetadata watermarkMetadata = TableWatermarkMetadata.builder()
                .eventTimeColumn("`timestamp`")
                .strategy("`timestamp`")
                .build();
        sourceTableMetadata = TableMetadata.builder()
                .name("t1")
                .columns(sourceTableColumns)
                .watermark(watermarkMetadata)
                .properties(sourceTableProperties)
                .build();

        task = new WarehouseJob();
        task.setName("streaming-warehouse-sql-task");
        task.setStreaming(true);
        task.setReportLineage(true);
    }

    protected void execute() throws Exception {
        String[] args = new String[] { "--taskConfig", encode(toJson(task)) };
        WarehouseJobBootstrap.run(args);
    }

}
