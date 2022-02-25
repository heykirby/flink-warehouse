package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

public class ProtobufMapConverterCodeGenerator implements ProtobufConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType keyType;
    private final LogicalType valueType;
    private final boolean ignoreDefaultValue;

    public ProtobufMapConverterCodeGenerator(Descriptors.FieldDescriptor fd, MapType type, boolean ignoreDefaultValue) {
        this.fd = fd;
        this.keyType = type.getKeyType();
        this.valueType = type.getValueType();
        this.ignoreDefaultValue = ignoreDefaultValue;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {
        return null;
    }

}
