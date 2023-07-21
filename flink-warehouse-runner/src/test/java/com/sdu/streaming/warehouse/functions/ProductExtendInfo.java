package com.sdu.streaming.warehouse.functions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

@FunctionHint(output = @DataTypeHint("ROW< id INT,  company STRING, company_address STRING >"))
public class ProductExtendInfo extends TableFunction<Row> {

    private static final Map<Long, Row> productCompaniesInfo;

    static {
        productCompaniesInfo = new HashMap<>();
        productCompaniesInfo.put(1L, Row.of(1, "山东济南娃哈哈有限公司", "山东济南"));
        productCompaniesInfo.put(2L, Row.of(2, "江苏连云港娃哈哈有限公司", "江苏连云港"));
        productCompaniesInfo.put(3L, Row.of(3, "河南郑州娃哈哈有限公司", "河南郑州"));
    }

    public void eval(long productId) {
        Row row = productCompaniesInfo.get(productId);
        if (row != null) {
            collect(row);
        }
    }

    public void eval(long productId, String name) {
        Row row = productCompaniesInfo.get(productId);
        if (row != null) {
            collect(row);
        }
    }

}
