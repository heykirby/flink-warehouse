package com.sdu.streaming.warehouse;

import com.sdu.streaming.warehouse.dto.WarehouseJob;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static java.lang.String.format;

public class SqlLineageITTest extends SqlBaseTest {


    @Test
    public void testSqlJoin() throws Exception {
        String productSinkTable = "CREATE TABLE product_sale_summary ( \n" +
                                  " order_id STRING, \n" +
                                  " id BIGINT, \n" +
                                  " sales DOUBLE, \n" +
                                  " name STRING, \n" +
                                  " sale_time TIMESTAMP_LTZ\n" +
                                  ") WITH ( \n" +
                                  " 'connector' = 'print' \n" +
                                  ")";
        String sql = format("INSERT INTO product_sale_summary \n" +
                            "SELECT \n" +
                            "   a.order_id as order_id, \n" +
                            "   b.id as id, \n" +
                            "   a.sales as sales, \n" +
                            "   b.name as name, \n" +
                            "   a.sale_time as sale_time \n" +
                            "FROM \n" +
                            "   %s a \n" +
                            "JOIN \n" +
                            "   %s b \n" +
                            "ON \n" +
                            " a.id = b.id", productSaleTableName, productInfoTableName);
        WarehouseJob warehouseJob = new WarehouseJob();
        warehouseJob.setStreaming(true);
        warehouseJob.setMaterials(Arrays.asList(productInfoTable, productSaleTable, productSinkTable));
        warehouseJob.setCalculates(Collections.singletonList(sql));
        WarehouseJobBootstrap.run(ofJobArgs(warehouseJob));
    }

}
