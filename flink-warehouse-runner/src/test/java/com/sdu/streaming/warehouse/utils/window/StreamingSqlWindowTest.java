package com.sdu.streaming.warehouse.utils.window;

import com.google.common.collect.Lists;
import com.sdu.streaming.warehouse.utils.StreamingSqlSqlTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class StreamingSqlWindowTest extends StreamingSqlSqlTest {

    @Before
    @Override
    public void setup() {
        super.setup();
        List<String> materials = Lists.newArrayList(task.getMaterials());
        materials.add(
                "CREATE TABLE t2 (window_start TIMESTAMP_LTZ(3), window_end TIMESTAMP_LTZ(3), price DOUBLE) WITH ('connector' = 'print')"
        );
        task.setMaterials(materials);
    }

    @Test
    public void testTumbleWindow() throws Exception {
        task.setCalculates(Lists.newArrayList(
                "INSERT INTO t2 SELECT window_start, window_end, SUM(price) FROM TABLE(TUMBLE(TABLE t1, DESCRIPTOR(ts), INTERVAL '2' SECONDS)) GROUP BY window_start, window_end"
        ));
        execute();
    }

    @Test
    public void testHopWindow() throws Exception {

    }

    @Test
    public void testCumulateWindow() throws Exception {

    }

}
