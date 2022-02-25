package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

public class ProtobufTypeConverterFactory {

    private ProtobufTypeConverterFactory() {

    }

    public static TypeConverterCodeGenerator getProtobufTypeConverterCodeGenerator(Map<String, String[]> fieldMappings, Descriptors.FieldDescriptor fd, LogicalType type, boolean ignoreDefaultValues) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case TINYINT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case CHAR:
            case VARCHAR:
                return new BasicTypeConverterCodeGenerator(fd, type);

            case ARRAY:
                return new ArrayTypeConverterCodeGenerator(fd, (ArrayType) type, ignoreDefaultValues);

            case MAP:
                return new MapTypeConverterCodeGenerator(fd, (MapType) type, ignoreDefaultValues);

            case ROW:
                return new RowTypeConverterCodeGenerator(fd.getMessageType(), (RowType) type, fieldMappings, ignoreDefaultValues);

            default:
                throw new UnsupportedOperationException("unsupported type converter, type: " + type.getTypeRoot());
        }
    }

    public static TypeConverterCodeGenerator getRowTypeConverterCodeGenerator(Descriptors.Descriptor descriptor, RowType rowType, Map<String, String[]> fieldMappings, boolean ignoreDefaultValue) {
        return new RowTypeConverterCodeGenerator(descriptor, rowType, fieldMappings, ignoreDefaultValue);
    }

}
