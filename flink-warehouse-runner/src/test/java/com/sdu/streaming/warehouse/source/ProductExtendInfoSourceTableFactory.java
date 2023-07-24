package com.sdu.streaming.warehouse.source;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

public class ProductExtendInfoSourceTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "product";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return new ProductExtendInfoLookupTableSource(context.getCatalogTable().getResolvedSchema());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

}
