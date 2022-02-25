package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getJavaFullName;

public class ProtobufTypeTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final List<Descriptors.FieldDescriptor> fds;
    private final Descriptors.Descriptor descriptor;
    private final RowType rowType;
    private final boolean ignoreDefaultValues;

    public ProtobufTypeTypeConverterCodeGenerator(Descriptors.Descriptor descriptor, RowType rowType, boolean ignoreDefaultValues) {
        this.descriptor = descriptor;
        this.fds = descriptor.getFields();
        this.rowType = rowType;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {
        /*
         * 代码:
         *    GenericRowData rowData$id = new GenericRowData(size);
         *    rowData$id.setField(index, object);
         *    resultVariable = rowData$id;
         * */
        // STEP1: 声明结果变量
        String rowDataVariable = String.format("rowData%d", getSerialId());
        StringBuilder sb = new StringBuilder();
        int index = 0, size = rowType.getFieldCount();
        String inputVariable = String.format("input%d",getSerialId());
        sb.append(getJavaFullName(descriptor)).append(" inputVariable").append(" = ").append(inputVariable).append(";");
        sb.append("GenericRowData ").append(rowDataVariable).append(" = new GenericRowData(").append(size).append(");");
        for (final String fieldName : rowType.getFieldNames()) {
            // STEP2: 获取列字段值
            Descriptors.FieldDescriptor subFd = fds.stream().filter(fd -> fd.getName().equals(fieldName)).findFirst().orElse(null);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            if (subFd == null) {
                throw new RuntimeException("cant find field statement, name: " + fieldName);
            }
            TypeConverterCodeGenerator codegen = getProtobufTypeConverterCodeGenerator(subFd, subType, ignoreDefaultValues);
            // 字段结果变量
            String fieldResultVariable = String.format("fieldResult%s", getSerialId());
            sb.append("Object ").append(fieldResultVariable).append(" = null;");
            // 字段原始值代码
            final String fieldCamelName = ProtobufUtils.getStrongCamelCaseJsonName(fieldName);
            final String fieldInputCode = getPrototbufFieldCode(subFd, fieldCamelName, inputVariable);
            sb.append(codegen.codegen(fieldResultVariable, fieldInputCode));
            // 赋值
            sb.append(rowDataVariable).append(".setField(").append(index).append(", ").append(fieldResultVariable).append(");");
            //
            index += 1;
        }
        sb.append(resultVariable).append(" = ").append(rowDataVariable).append(";");
        return sb.toString();
    }

    private static String getPrototbufFieldCode(Descriptors.FieldDescriptor fd, String fieldName, String protobufObjectVariable) {
        if (fd.isRepeated()) {
            return String.format("%s.get%sList()", protobufObjectVariable, fieldName);
        }
        if (fd.isMapField()) {
            return String.format("%s.get%sMap()", protobufObjectVariable, fieldName);
        }
        return String.format("%s.get%s()", protobufObjectVariable, fieldName);
    }
}
