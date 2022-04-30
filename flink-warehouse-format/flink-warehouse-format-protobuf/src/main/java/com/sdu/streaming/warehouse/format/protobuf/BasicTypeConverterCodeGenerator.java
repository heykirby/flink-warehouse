package com.sdu.streaming.warehouse.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;

import static java.lang.String.format;

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
                sb.append(format("%s = %s;", resultVariable, inputCode));
                break;
            case BYTE_STRING:
                sb.append(format("%s = %s.toByteArray();", resultVariable, inputCode));
                break;
            case STRING:
            case ENUM:
                sb.append(format("%s = StringData.fromString(%s.toString());", resultVariable, inputCode));
                break;

        }
        return sb.toString();
    }

}
