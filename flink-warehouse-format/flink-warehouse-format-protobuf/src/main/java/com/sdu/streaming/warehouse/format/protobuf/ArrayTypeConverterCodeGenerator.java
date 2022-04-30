package com.sdu.streaming.warehouse.format.protobuf;

import com.google.protobuf.Descriptors;
import com.sdu.streaming.warehouse.format.protobuf.utils.VariableUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;

import static com.sdu.streaming.warehouse.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.warehouse.format.protobuf.ProtobufUtils.getJavaType;
import static java.lang.String.format;

public class ArrayTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType type;
    private final Map<String, String[]> fieldMappings;
    private final boolean ignoreDefaultValues;

    public ArrayTypeConverterCodeGenerator(Descriptors.FieldDescriptor fd, ArrayType type, Map<String, String[]> fieldMappings, boolean ignoreDefaultValues) {
        this.fd = fd;
        this.type = type.getElementType();
        this.fieldMappings = fieldMappings;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {
        /*
         * 代码:
         *   List<JavaType> inputVariable = inputCode;
         *   Object[] result = new Object[inputVariable$1.size()];
         *   int index = 0;
         *   for (JavaType fieldValue : inputVariable) {
         *      Object elementResult = null;
         *   }
         *   resultVariable = new GenericArrayData(result);
         * */
        String elementType = getJavaType(fd);
        StringBuilder sb = new StringBuilder();
        String input = format("input$%d", VariableUtils.getSerialId());
        sb.append(format("List<%s> %s = %s;", elementType, input, inputCode));
        String res = String.format("res$%d", VariableUtils.getSerialId());
        sb.append(format("Object[] %s = new Object[%s.size()];", res, input));
        String el = format("el$%d", VariableUtils.getSerialId());
        String index = format("index$%d", VariableUtils.getSerialId());
        sb.append(format("int %s = 0;", index));
        sb.append(format("for (%s %s : %s) { ", elementType, el, input));
        String ret = String.format("ret$%d", VariableUtils.getSerialId());
        sb.append(format("Object %s = null;", ret));
        TypeConverterCodeGenerator codeGenerator = getProtobufTypeConverterCodeGenerator(fieldMappings, fd, type, ignoreDefaultValues);
        sb.append(codeGenerator.codegen(ret, el));
        sb.append(format("%s[%s++] = %s; }", index, res, ret));
        sb.append(format("%s = new GenericArrayData(%s);", resultVariable, res));
        return sb.toString();
    }

}
