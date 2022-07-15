package com.sdu.streaming.warehouse.utils.window;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.streaming.warehouse.dto.TableColumnMetadata;
import com.sdu.streaming.warehouse.dto.TableMetadata;
import com.sdu.streaming.warehouse.utils.StreamingSqlBaseTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.sdu.streaming.warehouse.utils.SqlCreateTableBuilder.buildCreateTableStatement;
import static java.lang.String.format;
import static org.apache.flink.shaded.guava30.com.google.common.collect.Lists.newArrayList;

public class StreamingSqlWindowITCase extends StreamingSqlBaseTest {

    private TableMetadata sinkTableMetadata;

    @Before
    @Override
    public void setup() {
        super.setup();
        // sink
        Map<String, String> sinkTableProperties = Maps.newHashMap();
        sinkTableProperties.put("connector", "print");
        List<TableColumnMetadata> sinkTableColumns = Lists.newArrayList(
                TableColumnMetadata.builder().name("window_start").type("TIMESTAMP_LTZ(3)").build(),
                TableColumnMetadata.builder().name("window_end").type("TIMESTAMP_LTZ(3)").build(),
                TableColumnMetadata.builder().name("sales").type("DOUBLE").build()
        );
        sinkTableMetadata = TableMetadata.builder()
                .name("t2")
                .columns(sinkTableColumns)
                .properties(sinkTableProperties)
                .build();

        task.setMaterials(newArrayList(
                buildCreateTableStatement(sourceTableMetadata),
                buildCreateTableStatement(sinkTableMetadata)
        ));
    }

    @Test
    public void testTumbleWindow() throws Exception {
        String insert = format(
                "INSERT INTO %s SELECT window_start, window_end, SUM(price)  FROM TABLE(TUMBLE(TABLE %s, DESCRIPTOR(`timestamp`), INTERVAL '2' SECONDS)) GROUP BY window_start, window_end",
                sinkTableMetadata.getName(),
                sourceTableMetadata.getName()
        );
        task.setCalculates(newArrayList(insert));
        execute();
    }

    @Test
    public void testHopWindow() throws Exception {

    }

    @Test
    public void testIncrementalWindow() throws Exception {

    }

}
