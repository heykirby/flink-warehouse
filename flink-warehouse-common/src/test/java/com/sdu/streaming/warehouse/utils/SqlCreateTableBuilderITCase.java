package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.dto.TableColumnMetadata;
import com.sdu.streaming.warehouse.dto.TableMetadata;
import com.sdu.streaming.warehouse.dto.TableWatermarkMetadata;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.sdu.streaming.warehouse.utils.SqlCreateTableBuilder.buildCreateTableStatement;

public class SqlCreateTableBuilderITCase {

    private String name;
    private List<TableColumnMetadata> columns;
    private Map<String, String> tableProperties;

    @Before
    public void setup() {
        name = "user";
        columns = Lists.newArrayList(
                TableColumnMetadata.builder().name("id").type("INT").nullable(false).build(),
                TableColumnMetadata.builder().name("name").type("VARCHAR").nullable(false).build(),
                TableColumnMetadata.builder().name("phone").type("BIGINT").nullable(true).build(),
                TableColumnMetadata.builder().name("`timestamp`").type("Timestamp(3)").nullable(false).build()
        );
        tableProperties = Maps.newHashMap();
        tableProperties.put("connector", "datagen");
        tableProperties.put("fields.id.min", "100000");
        tableProperties.put("fields.id.max", "200000");
    }

    @Test
    public void testCreateTableWithPrimaryKey() {
        TableMetadata tableMetadata = TableMetadata.builder()
                .name(name)
                .columns(columns)
                .primaryKeys("id")
                .properties(tableProperties)
                .build();
        System.out.println(buildCreateTableStatement(tableMetadata));
    }


    @Test
    public void testCreateTableWithWatermark() {
        TableWatermarkMetadata watermarkMetadata = TableWatermarkMetadata.builder()
                .eventTimeColumn("`timestamp`")
                .strategy("`timestamp` - INTERVAL '5' SECOND")
                .build();
        TableMetadata tableMetadata = TableMetadata.builder()
                .name(name)
                .columns(columns)
                .watermark(watermarkMetadata)
                .properties(tableProperties)
                .build();
        System.out.println(buildCreateTableStatement(tableMetadata));
    }


    @Test
    public void testCreateTableWithAllConstraint() {
        TableWatermarkMetadata watermarkMetadata = TableWatermarkMetadata.builder()
                .eventTimeColumn("`timestamp`")
                .strategy("`timestamp` - INTERVAL '5' SECOND")
                .build();
        TableMetadata tableMetadata = TableMetadata.builder()
                .name(name)
                .columns(columns)
                .watermark(watermarkMetadata)
                .primaryKeys("id, name")
                .properties(tableProperties)
                .build();
        System.out.println(buildCreateTableStatement(tableMetadata));
    }
}
