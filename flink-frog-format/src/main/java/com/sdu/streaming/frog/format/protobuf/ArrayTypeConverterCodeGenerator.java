package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getJavaType;

public class ArrayTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType elementType;
    private final boolean ignoreDefaultValues;

    public ArrayTypeConverterCodeGenerator(Descriptors.FieldDescriptor fd, ArrayType type, boolean ignoreDefaultValues) {
        this.fd = fd;
        this.elementType = type.getElementType();
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
        StringBuilder sb = new StringBuilder();
        String inputVariable = String.format("input%d", getSerialId());
        sb.append("List<").append(getJavaType(fd)).append("> ").append(inputVariable).append(" = ").append(inputCode).append(";");
        String arrayResultVariable = String.format("arrayResultVariable%d", getSerialId());
        sb.append("Object[] ").append(arrayResultVariable).append(" = ").append("new Object[").append(inputVariable).append(".size()];");
        String element = String.format("element%d", getSerialId());
        sb.append("int index = 0;");
        sb.append("for (").append(getJavaType(fd)).append(" ").append(element).append(" : ").append(inputVariable).append(") { ");
        String elementResult = String.format("elementResult%d", getSerialId());
        sb.append("Object ").append(elementResult).append(" = null;");
        TypeConverterCodeGenerator codeGenerator = getProtobufTypeConverterCodeGenerator(fd, elementType, ignoreDefaultValues);
        sb.append(codeGenerator.codegen(elementResult, element));
        sb.append(arrayResultVariable).append("[index++] = ").append(elementResult).append(";");
        sb.append("}");
        sb.append(resultVariable).append(" = = new GenericArrayData(").append(arrayResultVariable).append(");");
        return sb.toString();
    }

}
