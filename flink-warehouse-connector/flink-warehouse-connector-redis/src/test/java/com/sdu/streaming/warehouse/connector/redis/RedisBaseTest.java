package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.table.api.EnvironmentSettings;
import org.junit.Before;

public abstract class RedisBaseTest {

    protected EnvironmentSettings streamSettings;


    protected String productMsgTable;
    protected String productMsgTableName;

    // 产品信息表(维度表)
    protected String productRedisTable;
    protected String productRedisTableName;

    // 产品售卖表
    protected String saleTable;
    protected String saleTableName;

    // 产品售卖汇总表
    protected String saleSummaryTable;
    protected String saleSummaryTableName;

    @Before
    public void setup() {
        this.streamSettings = EnvironmentSettings.inStreamingMode();

        this.productMsgTable = "CREATE TABLE product_info ( \n" +
                               "  id BIGINT, \n" +
                               "  name STRING, \n" +
                               "  address STRING, \n" +
                               "  PRIMARY KEY id NOT ENFORCED \n" +
                               ") WITH ( \n" +
                               "  'connector' = 'datagen', \n" +
                               "  'number-of-rows' = '10', \n" +
                               "  'rows-per-second' = '2', \n" +
                               "  'fields.id.min' = '1', \n" +
                               "  'fields.id.max' = '10'\n" +
                               ")";
        this.productMsgTableName = "product_info";

        this.productRedisTable = "CREATE TABLE product_dim_info ( \n" +
                                 "  id BIGINT, \n" +
                                 "  name STRING, \n" +
                                 "  address STRING, \n" +
                                 "  PRIMARY KEY id NOT ENFORCED \n" +
                                 ") WITH ( \n" +
                                 "   'connector' = 'redis',  \n" +
                                 "   'redis-address' = 'redis://127.0.0.1:6379', \n" +
                                 "   'redis-key-prefix' = 'product', \n" +
                                 "   'redis-write-batch-size' = '1' \n" +
                                 ")";
        this.productRedisTableName = "product_dim_info";

        this.saleTable = "CREATE TABLE sale ( \n" +
                         "  id BIGINT, \n" +
                         "  order_id STRING, \n" +
                         "  sales DOUBLE, \n" +
                         "  sale_time AS PROCTIME(), \n" +
                         "  PRIMARY KEY order_id NOT ENFORCED \n" +
                         ") WITH ( \n" +
                         "  'connector' = 'datagen', \n" +
                         "  'number-of-rows' = '10', \n" +
                         "  'rows-per-second' = '2', \n" +
                         "  'fields.id.min' = '1', \n" +
                         "  'fields.id.max' = '10'\n" +
                         ")";
        this.saleTableName = "sale";

        this.saleSummaryTable = "CREATE TABLE sale_summary ( \n" +
                                "   id BIGINT, \n" +
                                "   sales DOUBLE, \n" +
                                "   window_start TIMESTAMP_LTZ, \n" +
                                "   window_end TIMESTAMP_LTZ, \n" +
                                "   PRIMARY KEY id, window_start, window_end NOT ENFORCED \n" +
                                ") WITH ( \n" +
                                "   'connector' = 'redis', \n" +
                                "   'redis-address' = 'redis://127.0.0.1:6379', \n" +
                                "   'redis-key-prefix' = 'PS', \n" +
                                "   'redis-write-batch-size' = '1'" +
                                ")";
        this.saleSummaryTableName = "sale_summary";
    }

}
