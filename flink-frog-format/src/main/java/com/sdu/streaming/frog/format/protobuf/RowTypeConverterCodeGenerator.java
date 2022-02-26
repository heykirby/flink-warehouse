package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getJavaFullName;
import static java.lang.String.format;

public class RowTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final List<Descriptors.FieldDescriptor> fds;
    private final Descriptors.Descriptor descriptor;
    private final RowType rowType;
    private final boolean ignoreDefaultValues;
    // NOTE:
    // Row<name type, Row<name, type>>: 嵌套类型Row, fieldMapping取相对路径
    private final Map<String, String[]> fieldMappings;

    public RowTypeConverterCodeGenerator(Descriptors.Descriptor descriptor, RowType rowType, Map<String, String[]> fieldMappings, boolean ignoreDefaultValues) {
        this.descriptor = descriptor;
        this.fds = descriptor.getFields();
        this.fieldMappings = fieldMappings;
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
//            Descriptors.FieldDescriptor subFd = fds.stream().filter(fd -> fd.getName().equals(fieldName)).findFirst().orElse(null);
            Descriptors.FieldDescriptor subFd = getFieldDescriptor(fieldName);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            TypeConverterCodeGenerator codegen = getProtobufTypeConverterCodeGenerator(fieldMappings, subFd, subType, ignoreDefaultValues);
            // 字段结果变量
            String ret = format("ret$%s", getSerialId());
            sb.append(format("Object %s = null;", ret));
            // 字段值
//            final String fieldCamelName = ProtobufUtils.getStrongCamelCaseJsonName(fieldName);
//            final String fieldInputCode = getPrototbufFieldValueCode(subFd, fieldCamelName, input);
            final String fileValueCode = getPrototbufFieldValueCode(subFd, fieldName, input);
            sb.append(codegen.codegen(ret, fileValueCode));

            sb.append(format("%s.setField(%d, %s);", rowData, index, ret));
            index += 1;
        }
        sb.append(format("%s = %s;", resultVariable, rowData));
        return sb.toString();
    }

    private Descriptors.FieldDescriptor getFieldDescriptor(String fieldName) {
        String[] fields = fieldMappings.get(fieldName);
        Descriptors.FieldDescriptor ret = null;
        List<Descriptors.FieldDescriptor> fds = this.fds;
        for(int i = 0; i < fields.length; ++i) {
            String field = fields[i];
            ret = fds.stream().filter(fd -> fd.getName().equals(field)).findFirst().orElse(null);
            if (ret == null) {
                throw new RuntimeException("cant find field descriptor for path: " + join(fields, i));
            }
            if (i != fields.length - 1) {
                fds = ret.getMessageType().getFields();
                ret = null;
            }
        }
        if (ret == null) {
            throw new RuntimeException("cant find field descriptor for path: " + join(fields, fields.length - 1));
        }
        return ret;
    }

    private static String join(String[] fields, int offset) {
        StringBuilder sb = new StringBuilder("$");
        for (int i = 0; i <= offset; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(fields[i]);
        }
        return sb.toString();
    }

    private String getPrototbufFieldValueCode(Descriptors.FieldDescriptor fd, String fieldName, String protobufObjectVariable) {
        String[] fields = fieldMappings.get(fieldName);
        // index: (0, fields.length - 2) 必需为Message类型
        // todo: 若是中间节点取值null, 存在空指针问题
        StringBuilder sb = new StringBuilder(protobufObjectVariable);
        for (int i = 0; i < fields.length; ++i) {
            String field = ProtobufUtils.getStrongCamelCaseJsonName(fields[i]);
            if (i == fields.length - 1) {
                if (fd.isMapField()) {
                    sb.append(format(".get%sMap()", field));
                    return sb.toString();
                }
                if (fd.isRepeated()) {
                    sb.append(format(".get%sList()", field));
                    return sb.toString();
                }
                sb.append(format(".get%s()", field));
                return sb.toString();
            }
            sb.append(format(".get%s()", field));
        }
        return sb.toString();
    }

//    private static String getPrototbufFieldValueCode(Descriptors.FieldDescriptor fd, String fieldName, String protobufObjectVariable) {
//        if (fd.isRepeated()) {
//            return format("%s.get%sList()", protobufObjectVariable, fieldName);
//        }
//        if (fd.isMapField()) {
//            return format("%s.get%sMap()", protobufObjectVariable, fieldName);
//        }
//        return format("%s.get%s()", protobufObjectVariable, fieldName);
//    }
}
