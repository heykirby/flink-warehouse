package com.sdu.streaming.warehouse.source;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;

public class ProductExtendInfoLookupTableSource implements LookupTableSource {

    private final ResolvedSchema schema;

    public ProductExtendInfoLookupTableSource(ResolvedSchema schema) {
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new ProductExtendInfoLookupTableSource(schema);
    }

    @Override
    public String asSummaryString() {
        return "product extend information";
    }

}
