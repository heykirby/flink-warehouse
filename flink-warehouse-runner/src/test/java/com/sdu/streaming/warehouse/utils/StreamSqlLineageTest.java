package com.sdu.streaming.warehouse.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.streaming.warehouse.dto.TableColumnMetadata;
import com.sdu.streaming.warehouse.dto.TableMetadata;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.sdu.streaming.warehouse.utils.SqlCreateTableBuilder.buildCreateTableStatement;
import static java.lang.String.format;
import static org.apache.flink.shaded.guava30.com.google.common.collect.Lists.newArrayList;

public class StreamSqlLineageTest extends StreamingSqlBaseTest{

    @Test
    public void testSimpleInsert() throws Exception {
        // sink
        Map<String, String> sinkTableProperties = Maps.newHashMap();
        sinkTableProperties.put("connector", "print");
        List<TableColumnMetadata> sinkTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("ids").type("BIGINT").build(),
                TableColumnMetadata.builder().name("names").type("STRING").build()
        );
        TableMetadata sinkTableMetadata = TableMetadata.builder()
                .name("t2")
                .columns(sinkTableColumns)
                .properties(sinkTableProperties)
                .build();

        task.setMaterials(newArrayList(
                buildCreateTableStatement(sourceTableMetadata),
                buildCreateTableStatement(sinkTableMetadata)
        ));

        String insert = format(
                "INSERT INTO %s SELECT id, name FROM %s",
                sinkTableMetadata.getName(),
                sourceTableMetadata.getName()
        );
        task.setCalculates(newArrayList(insert));
        execute();
    }

}
