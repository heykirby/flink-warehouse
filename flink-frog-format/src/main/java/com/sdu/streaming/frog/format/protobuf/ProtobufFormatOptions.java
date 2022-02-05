package com.sdu.streaming.frog.format.protobuf;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@PublicEvolving
public class ProtobufFormatOptions {

    public static final ConfigOption<String> PROTOBUF_CLASS =
            ConfigOptions.key("protbuf-class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option flag to describe protobuf class.");

    public static final ConfigOption<String> FIELD_MAPPING =
            ConfigOptions.key("field-mapping")
                .stringType()
                .noDefaultValue()
                .withDescription("Option flag to convert protobuf object to table column field.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<String> TIME_PATTERN =
            ConfigOptions.key("time-pattern")
                    .stringType()
                    .defaultValue("yyyy-MM-dd HH:mm:ss")
                    .withDescription("Option flag to convert time text to time.");

    private ProtobufFormatOptions() {

    }

}
