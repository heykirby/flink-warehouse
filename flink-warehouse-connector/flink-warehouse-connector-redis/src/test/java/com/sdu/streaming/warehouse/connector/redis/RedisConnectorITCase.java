package com.sdu.streaming.warehouse.connector.redis;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;

public class RedisConnectorITCase extends RedisBaseTest {


    @Before
    public void setup() {
        super.setup();
    }

    @Test
    public void testRedisTableAppendSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(productMsgTable);
        tEnv.executeSql(productRedisTable);
        String sql = format("INSERT INTO %s SELECT * FROM %s", productRedisTableName, productMsgTableName);
        TableResult tableResult = tEnv.executeSql(sql);
        // wait finish
        tableResult.await();
    }

    @Test
    public void testRedisTableRestrictSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(saleTable);
        tEnv.executeSql(saleSummaryTable);

        String restrictSql = format(
                "INSERT INTO %s SELECT id, sum(sales), window_start, window_end FROM TABLE(TUMBLE(TABLE %s, DESCRIPTOR(%s), INTERVAL '1' SECONDS)) GROUP BY id, window_start, window_end",
                saleSummaryTableName,
                saleTableName,
                "sale_time"
        );

        tEnv.executeSql(restrictSql).await();
    }

    @Test
    public void testLookupJoinRedisTable() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(saleTable);
        tEnv.executeSql(productRedisTableName);

        String lookupJoinSql = format(
                "SELECT oid, p.id, p.name, p.address, sales, sale_time FROM %s AS s LEFT JOIN %s FOR SYSTEM_TIME AS OF s.sale_time AS p ON s.id = p.id",
                saleTableName,
                productRedisTableName
        );

        TableResult tableResult = tEnv.executeSql(lookupJoinSql);
        tableResult.collect().forEachRemaining(System.out::println);
    }
}
