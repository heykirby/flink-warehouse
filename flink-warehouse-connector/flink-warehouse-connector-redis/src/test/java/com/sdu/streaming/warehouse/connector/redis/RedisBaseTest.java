package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.dto.TableColumnMetadata;
import com.sdu.streaming.warehouse.dto.TableMetadata;
import com.sdu.streaming.warehouse.dto.TableWatermarkMetadata;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.table.api.EnvironmentSettings;
import org.junit.Before;

import java.util.List;
import java.util.Map;

public abstract class RedisBaseTest {

    protected EnvironmentSettings streamSettings;

    // 产品信息表(维度表)
    protected String productMessageRedisTableName;
    protected TableMetadata productMessageRedisTableMetadata;

    protected String productMessageSourceTableName;
    protected TableMetadata productMessageSourceTableMetadata;

    // 产品售卖表
    protected String productSaleTableName;
    protected TableMetadata productSaleTableMetadata;

    // 产品售卖汇总表
    protected String productSaleSummaryTableName;
    protected TableMetadata productSaleSummaryTableMetadata;

    @Before
    public void setup() {
        this.streamSettings = EnvironmentSettings.inStreamingMode();

        // redis table
        List<TableColumnMetadata> productMessageTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("id").type("BIGINT").build(),
                TableColumnMetadata.builder().name("name").type("STRING").build(),
                TableColumnMetadata.builder().name("address").type("STRING").build()
        );
        Map<String, String> productMessageRedisTableProperties = Maps.newHashMap();
        productMessageRedisTableProperties.put("connector", "redis");
        productMessageRedisTableProperties.put("redis-address", "redis://127.0.0.1:6379");
        productMessageRedisTableProperties.put("redis-key-prefix", "CN");
        productMessageRedisTableProperties.put("redis-write-batch-size", "1");
        this.productMessageRedisTableName = "product";
        this.productMessageRedisTableMetadata = TableMetadata.builder()
                .name(this.productMessageRedisTableName)
                .columns(productMessageTableColumns)
                .primaryKeys("id")
                .properties(productMessageRedisTableProperties)
                .build();

        Map<String, String> productTableProperties = Maps.newHashMap();
        productTableProperties.put("connector", "datagen");
        productTableProperties.put("number-of-rows", "10");
        productTableProperties.put("rows-per-second", "2");
        productTableProperties.put("fields.id.min", "1");
        productTableProperties.put("fields.id.max", "10");

        this.productMessageSourceTableName = "product_message";
        this.productMessageSourceTableMetadata = TableMetadata.builder()
                .name(this.productMessageSourceTableName)
                .columns(productMessageTableColumns)
                .primaryKeys("id")
                .properties(productTableProperties)
                .build();

        // productSale table
        List<TableColumnMetadata> productSaleTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("id").type("BIGINT").build(),
                TableColumnMetadata.builder().name("oid").type("STRING").build(),
                TableColumnMetadata.builder().name("sales").type("INT").build(),
                TableColumnMetadata.builder().name("sale_time").type("AS PROCTIME()").nullable(true).build()
        );
        productTableProperties.put("rows-per-second", "2");
        this.productSaleTableName = "product_sales";
        this.productSaleTableMetadata = TableMetadata.builder()
                .name(this.productSaleTableName)
                .columns(productSaleTableColumns)
                .primaryKeys("oid")
                .properties(productTableProperties)
                .build();

        // product sale summary table
        List<TableColumnMetadata> productSaleSummaryTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("id").type("BIGINT").build(),
                TableColumnMetadata.builder().name("sales").type("INT").build(),
                TableColumnMetadata.builder().name("window_start").type("TIMESTAMP_LTZ").build(),
                TableColumnMetadata.builder().name("window_end").type("TIMESTAMP_LTZ").build()
        );
        Map<String, String> productSaleSummaryTableProperties = Maps.newHashMap();
        productSaleSummaryTableProperties.put("connector", "redis");
        productSaleSummaryTableProperties.put("redis-address", "redis://127.0.0.1:6379");
        productSaleSummaryTableProperties.put("redis-key-prefix", "PS");
        productSaleSummaryTableProperties.put("redis-write-batch-size", "1");
        this.productSaleSummaryTableName = "product_sale_summary";
        this.productSaleSummaryTableMetadata = TableMetadata.builder()
                .name(this.productSaleSummaryTableName)
                .columns(productSaleSummaryTableColumns)
                .primaryKeys("id, window_start, window_end")
                .properties(productSaleSummaryTableProperties)
                .build();
    }

}
