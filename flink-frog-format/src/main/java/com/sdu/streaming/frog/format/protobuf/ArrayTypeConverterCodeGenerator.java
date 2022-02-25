package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Map;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getJavaType;
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
        String input = format("input$%d", getSerialId());
        sb.append(format("List<%s> %s = %s;", elementType, input, inputCode));
        String res = format("res$%d", getSerialId());
        sb.append(format("Object[] %s = new Object[%s.size()];", res, input));
        String el = format("el$%d", getSerialId());
        sb.append("int index = 0;");
        sb.append(format("for (%s %s : %s) { ", elementType, el, input));
        String ret = format("ret$%d", getSerialId());
        sb.append(format("Object %s = null;", ret));
        TypeConverterCodeGenerator codeGenerator = getProtobufTypeConverterCodeGenerator(fieldMappings, fd, type, ignoreDefaultValues);
        sb.append(codeGenerator.codegen(ret, el));
        sb.append(format("%s[index++] = %s; }", res, ret));
        sb.append(format("%s = new GenericArrayData(%s);", resultVariable, res));
        return sb.toString();
    }

}
