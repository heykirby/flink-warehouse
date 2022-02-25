package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;

public class BasicTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType type;

    public BasicTypeConverterCodeGenerator(Descriptors.FieldDescriptor fd, LogicalType type) {
        this.fd = fd;
        this.type = type;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {
        StringBuilder sb = new StringBuilder();
        switch (fd.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                sb.append(resultVariable).append(" = ").append(inputCode).append(";");
                break;
            case BYTE_STRING:
                sb.append(resultVariable).append(" = ").append(inputCode).append("toByteArray();");
                break;
            case STRING:
            case ENUM:
                sb.append(resultVariable).append(" = ").append("StringData.fromString(").append(inputCode).append(".toString());");
                break;

        }
        return sb.toString();
    }

}
