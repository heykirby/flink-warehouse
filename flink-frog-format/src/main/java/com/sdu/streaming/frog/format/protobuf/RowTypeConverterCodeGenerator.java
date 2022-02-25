package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getJavaFullName;
import static java.lang.String.format;

public class RowTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final List<Descriptors.FieldDescriptor> fds;
    private final Descriptors.Descriptor descriptor;
    private final RowType rowType;
    private final boolean ignoreDefaultValues;

    public RowTypeConverterCodeGenerator(Descriptors.Descriptor descriptor, RowType rowType, boolean ignoreDefaultValues) {
        this.descriptor = descriptor;
        this.fds = descriptor.getFields();
        this.rowType = rowType;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {

        StringBuilder sb = new StringBuilder();
        int index = 0, size = rowType.getFieldCount();
        String input = format("input$%d",getSerialId());
        String rowData = format("row$%d", getSerialId());
        sb.append(format("%s %s = %s;", getJavaFullName(descriptor), input, inputCode));
        sb.append(format("GenericRowData %s = new GenericRowData(%d);", rowData, size));
        for (final String fieldName : rowType.getFieldNames()) {
            // STEP2: 获取列字段值
            Descriptors.FieldDescriptor subFd = fds.stream().filter(fd -> fd.getName().equals(fieldName)).findFirst().orElse(null);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            if (subFd == null) {
                throw new RuntimeException("cant find field statement, name: " + fieldName);
            }
            TypeConverterCodeGenerator codegen = getProtobufTypeConverterCodeGenerator(subFd, subType, ignoreDefaultValues);
            // 字段结果变量
            String ret = format("ret$%s", getSerialId());
            sb.append(format("Object %s = null;", ret));
            // 字段值
            final String fieldCamelName = ProtobufUtils.getStrongCamelCaseJsonName(fieldName);
            final String fieldInputCode = getPrototbufFieldValueCode(subFd, fieldCamelName, input);
            sb.append(codegen.codegen(ret, fieldInputCode));

            sb.append(format("%s.setField(%d, %s);", rowData, index, ret));
            index += 1;
        }
        sb.append(format("%s = %s;", resultVariable, rowData));
        return sb.toString();
    }


    private static String getPrototbufFieldValueCode(Descriptors.FieldDescriptor fd, String fieldName, String protobufObjectVariable) {
        if (fd.isRepeated()) {
            return format("%s.get%sList()", protobufObjectVariable, fieldName);
        }
        if (fd.isMapField()) {
            return format("%s.get%sMap()", protobufObjectVariable, fieldName);
        }
        return format("%s.get%s()", protobufObjectVariable, fieldName);
    }
}
