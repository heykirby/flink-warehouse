package com.sdu.streaming.frog.format.protobuf;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@PublicEvolving
public class ProtobufFormatOptions {

    public static final ConfigOption<String> PROTOBUF_CLASS =
            ConfigOptions.key("protobuf-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option flag to describe protobuf class.");

    public static final ConfigOption<String> PROTOBUF_FIELD_MAPPING =
            ConfigOptions.key("protobuf-field-mapping")
                .stringType()
                .noDefaultValue()
                .withDescription("Option flag to convert protobuf object to table column field.");

    public static final ConfigOption<Boolean> PROTOBUF_IGNORE_PARSE_ERROR =
            ConfigOptions.key("protobuf-ignore-parse-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<Boolean> PROTOBUF_IGNORE_DEFAULT_VALUE =
            ConfigOptions.key("protobuf-ignore-default-value")
                    .booleanType()
                    .defaultValue(false);

    private ProtobufFormatOptions() {

    }

}
