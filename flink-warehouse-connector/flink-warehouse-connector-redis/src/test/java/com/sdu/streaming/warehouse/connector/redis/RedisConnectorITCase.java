package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import static com.sdu.streaming.warehouse.utils.SqlCreateTableBuilder.buildCreateTableStatement;
import static java.lang.String.format;

public class RedisConnectorITCase extends RedisBaseTest {

    private String productMessageSourceTable;
    private String productMessageRedisTable;
    private String productSaleTable;
    private String productSaleSummaryTable;

    @Before
    public void setup() {
        super.setup();
        // product message table
        productMessageSourceTable = buildCreateTableStatement(productMessageSourceTableMetadata);
        productMessageRedisTable = buildCreateTableStatement(productMessageRedisTableMetadata);
        // product sale table
        productSaleTable = buildCreateTableStatement(productSaleTableMetadata);
        // product sale summary table
        productSaleSummaryTable = buildCreateTableStatement(productSaleSummaryTableMetadata);
    }

    @Test
    public void testRedisTableAppendSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(productMessageSourceTable);
        tEnv.executeSql(productMessageRedisTable);

        TableResult tableResult = tEnv.executeSql(format("INSERT INTO %s SELECT * FROM %s", productMessageRedisTableName, productMessageSourceTableName));
        // wait finish
        tableResult.await();
    }

    @Test
    public void testRedisTableRestrictSink() throws Exception {

    }

    @Test
    public void testLookupJoinRedisTable() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(productSaleTable);
        tEnv.executeSql(productMessageRedisTable);

        String lookupJoinSql = format(
                "SELECT oid, p.id, p.name, p.address, sales, sale_time FROM %s LEFT JOIN %s FOR SYSTEM_TIME AS OF s.sale_time AS p ON s.id = p.id",
                productSaleTableName,
                productMessageRedisTableName
        );

        TableResult tableResult = tEnv.executeSql(lookupJoinSql);
        tableResult.collect().forEachRemaining(System.out::println);
    }
}
