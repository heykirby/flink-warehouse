package com.sdu.streaming.frog.format.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.sdu.streaming.frog.format.protobuf.ProtobufFormatOptions.*;

public class ProtobufFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "protobuf";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String clazz = formatOptions.get(PROTOBUF_CLASS);
        final String fieldMapping = formatOptions.get(PROTOBUF_FIELD_MAPPING);
        final boolean ignoreParseError = formatOptions.get(PROTOBUF_IGNORE_PARSE_ERROR);
        final boolean ignoreDefaultValue = formatOptions.get(PROTOBUF_IGNORE_DEFAULT_VALUE);

        return new DecodingFormat<DeserializationSchema<RowData>>() {

            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType produceDataType) {
                final RowType rowType = (RowType) produceDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo = context.createTypeInformation(produceDataType);
                // TODO: field mapping
                return new ProtobufRowDataDeserializationSchema(rowType, rowDataTypeInfo, clazz,
                        new HashMap<>(), ignoreParseError, ignoreDefaultValue);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        throw new UnsupportedOperationException("unsupported serialize object to protobuf bytes.");
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROTOBUF_CLASS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROTOBUF_FIELD_MAPPING);
        options.add(PROTOBUF_IGNORE_PARSE_ERROR);
        options.add(PROTOBUF_IGNORE_DEFAULT_VALUE);
        return options;
    }

}
