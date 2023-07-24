package com.sdu.streaming.warehouse;

import com.sdu.streaming.warehouse.dto.WarehouseJob;
import com.sdu.streaming.warehouse.utils.Base64Utils;
import com.sdu.streaming.warehouse.utils.JsonUtils;
import org.junit.Before;

public abstract class SqlBaseTest {

    protected String productInfoTable;
    protected String productInfoTableName;
    protected String productSaleTable;
    protected String productSaleTableName;

    @Before
    public void setup() throws Exception {
        this.productInfoTable = "CREATE TABLE product_info ( \n" +
                                "  id BIGINT, \n" +
                                "  name STRING, \n" +
                                "  address STRING, \n" +
                                "  proc_time AS PROCTIME(), \n" +
                                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                                ") WITH ( \n" +
                                "  'connector' = 'datagen', \n" +
                                "  'number-of-rows' = '10', \n" +
                                "  'rows-per-second' = '2', \n" +
                                "  'fields.id.min' = '1', \n" +
                                "  'fields.id.max' = '10'\n" +
                                ")";
        this.productInfoTableName = "product_info";

        this.productSaleTable = "CREATE TABLE product_sale ( \n" +
                                "  id BIGINT, \n" +
                                "  order_id STRING, \n" +
                                "  sales DOUBLE, \n" +
                                "  sale_time TIMESTAMP_LTZ, \n" +
                                "  PRIMARY KEY (order_id) NOT ENFORCED \n" +
                                ") WITH ( \n" +
                                "  'connector' = 'datagen', \n" +
                                "  'number-of-rows' = '10', \n" +
                                "  'rows-per-second' = '2', \n" +
                                "  'fields.id.min' = '1', \n" +
                                "  'fields.id.max' = '10'\n" +
                                ")";
        this.productSaleTableName = "product_sale";
    }

    protected String[] ofJobArgs(WarehouseJob job) throws Exception {
        String jobJson = JsonUtils.toJson(job);
        String decodeJob = Base64Utils.encode(jobJson);
        return new String[] {"--taskConfig", decodeJob};
    }

}
