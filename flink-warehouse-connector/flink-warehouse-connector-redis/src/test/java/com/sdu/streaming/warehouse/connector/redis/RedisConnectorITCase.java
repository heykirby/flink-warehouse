package com.sdu.streaming.warehouse.connector.redis;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.guava30.com.google.common.collect.Lists.newArrayList;

public class RedisConnectorITCase extends RedisBaseTest {

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(new Configuration())
                            .build());

    private String sourceTable;
    private String orderTable;
    private String redisTable;

    @Before
    public void setup() {
        super.setup();
        // source table
        Map<String, String> sourceProperties = Maps.newHashMap();
        sourceProperties.put("connector", "datagen");
        sourceProperties.put("number-of-rows", "2");
        sourceProperties.put("fields.pid.min", "100");
        sourceProperties.put("fields.pid.max", "100");
        sourceTable = createTableDDL(
                "t1",
                newArrayList(Tuple2.of("pid", "INT"), Tuple2.of("name", "STRING"), Tuple2.of("price", "DOUBLE")),
                newArrayList(),
                sourceProperties
        );

        // order table
        Map<String, String> orderProperties = Maps.newHashMap();
        orderProperties.put("connector", "datagen");
        orderProperties.put("number-of-rows", "2");
        orderProperties.put("fields.pid.min", "100");
        orderProperties.put("fields.pid.max", "100");
        orderTable = createTableDDL(
                "s1",
                newArrayList(Tuple2.of("id", "STRING"), Tuple2.of("pid", "INT"), Tuple2.of("ptime", "AS PROCTIME()")),
                null,
                orderProperties
        );

        // redis table
        Map<String, String> redisProperties = Maps.newHashMap();
        redisProperties.put("connector", "redis");
        redisProperties.put("redis-address", "redis://127.0.0.1:6379");
        redisProperties.put("redis-key-prefix", "pro-");
        redisProperties.put("redis-write-batch-size", "1");
        redisTable = createTableDDL(
                "t2",
                newArrayList(Tuple2.of("pid", "INT"), Tuple2.of("name", "STRING"), Tuple2.of("price", "DOUBLE")),
                newArrayList("pid"),
                redisProperties
        );
    }

    @Test
    public void testRedisTableSink() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(sourceTable);
        tEnv.executeSql(redisTable);

        TableResult tableResult = tEnv.executeSql("INSERT INTO t2 SELECT * FROM t1");
        // wait finish
        tableResult.await();
    }

    @Test
    public void testLookupJoinRedisTable() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);
        tEnv.executeSql(orderTable);
        tEnv.executeSql(redisTable);

        TableResult tableResult = tEnv.executeSql("SELECT id, s1.pid, name, price FROM s1 LEFT JOIN t2 FOR SYSTEM_TIME AS OF s1.ptime ON s1.pid = t2.pid");
        tableResult.collect().forEachRemaining(System.out::println);
    }


    private static String createTableDDL(String name,
                                         List<Tuple2<String, String>> columns,
                                         List<String> primaryKeys,
                                         Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        sb.append("CREATE TABLE ").append(name).append(" ( \n");
        // columns
        for (Tuple2<String, String> column : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(", \n");
            }
            sb.append("\t").append(column.f0).append(" ").append(column.f1);
        }
        // primary key
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String keys = StringUtils.join(primaryKeys, ", ");
            sb.append(", \n").append("\tPRIMARY KEY (").append(keys).append(") NOT ENFORCED");
        }
        // with
        sb.append(" \n ) WITH ( \n");
        first = true;
        for (Map.Entry<String, String> prop : properties.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", \n");
            }
            sb.append("\t'").append(prop.getKey()).append("' = '").append(prop.getValue()).append("'");
        }
        sb.append(" \n)");

        return sb.toString();
    }
}
