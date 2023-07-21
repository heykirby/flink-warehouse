package com.sdu.streaming.warehouse;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import com.sdu.streaming.warehouse.dto.WarehouseJob;

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
                            "   %s b \n" +
                            "JOIN \n" +
                            "   %s a \n" +
                            "ON \n" +
                            " a.id = b.id", productInfoTableName, productSaleTableName);
        WarehouseJob warehouseJob = new WarehouseJob();
        warehouseJob.setStreaming(true);
        warehouseJob.setMaterials(Arrays.asList(productInfoTable, productSaleTable, productSinkTable));
        warehouseJob.setCalculates(Collections.singletonList(sql));
        WarehouseJobBootstrap.run(ofJobArgs(warehouseJob));
    }

    @Test
    public void testSqlTableFunction() throws Exception {
        String productSinkTable = "CREATE TABLE product_info_summary ( \n" +
                                  " id BIGINT, \n" +
                                  " name STRING, \n" +
                                  " address STRING, \n" +
                                  " company STRING, \n" +
                                  " company_address STRING \n" +
                                  ") WITH ( \n" +
                                  " 'connector' = 'print' \n" +
                                  ")";
        String productExtendInfoTable = "CREATE FUNCTION IF NOT EXISTS product_extend_info \n" +
                                        "AS 'com.sdu.streaming.warehouse.functions.ProductExtendInfo'";
        String sql = format("INSERT INTO product_info_summary \n" +
                "   SELECT \n" +
                "       b.company as company, \n" +
                "       a.id as id, \n" +
                "       a.name as name, \n" +
                "       a.address as address, \n" +
                "       b.company_address as company_address \n" +
                "   FROM \n" +
                "       %s a, \n" +
                "   LATERAL TABLE(product_extend_info(a.id)) b", productInfoTableName);
        WarehouseJob warehouseJob = new WarehouseJob();
        warehouseJob.setStreaming(true);
        warehouseJob.setMaterials(Arrays.asList(productExtendInfoTable, productInfoTable, productSinkTable));
        warehouseJob.setCalculates(Collections.singletonList(sql));
        WarehouseJobBootstrap.run(ofJobArgs(warehouseJob));
    }

    @Test
    public void testSqlLookupJoin() throws Exception {

    }

}
