package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;

public class MapTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType keyType;
    private final LogicalType valueType;
    private final boolean ignoreDefaultValue;

    public MapTypeConverterCodeGenerator(Descriptors.FieldDescriptor fd, MapType type, boolean ignoreDefaultValue) {
        this.fd = fd;
        this.keyType = type.getKeyType();
        this.valueType = type.getValueType();
        this.ignoreDefaultValue = ignoreDefaultValue;
    }

    @Override
    public String codegen(String resultVariable, String inputCode) {
        /*
         * 代码:
         * Map<JavaType, JavaType> inputVariable = inputCode;
         * Map<Object, Object> result = new HashMap<>();
         * for(Map.Entry<JavaType, JavaType> entry : inputVariable.entrySet()) {
         *      // key
         *      // value
         *      result.put();
         * }
         * resultVariable = new GenericMapData(result);
         * */
        Descriptors.FieldDescriptor keyFd = fd.getMessageType().findFieldByName("key");
        Descriptors.FieldDescriptor valueFd = fd.getMessageType().findFieldByName("value");
        StringBuilder sb = new StringBuilder();
        String inputVariable = String.format("inputVariable%d", getSerialId());
        String keyJavaType = ProtobufUtils.getJavaType(keyFd);
        String valueJavaType = ProtobufUtils.getJavaType(valueFd);
        sb.append("Map<").append(keyJavaType).append(", ").append(valueJavaType).append("> ").append(inputVariable)
                .append(" = ").append(inputCode).append(";");
        String mapResult = String.format("mapResult%d", getSerialId());
        sb.append("Map<Object, Object> ").append(mapResult).append(" = new HashMap<>();");
        String entry = String.format("entry%d", getSerialId());
        sb.append("for(Map.Entry<").append(keyJavaType).append(", ").append(valueJavaType).append("> ").append(entry)
                .append(" : ").append(inputVariable).append(".entrySet()) { ");
        String key = String.format("key%d", getSerialId());
        String value = String.format("value%d", getSerialId());
        sb.append("Object ").append(key).append(" = null;");
        sb.append("Object ").append(value).append(" = null;");
        TypeConverterCodeGenerator keyCodeGenerator = getProtobufTypeConverterCodeGenerator(keyFd, keyType, ignoreDefaultValue);
        sb.append(keyCodeGenerator.codegen(key, String.format("%s.getKey()", entry)));
        TypeConverterCodeGenerator valueCodeGenerator = getProtobufTypeConverterCodeGenerator(valueFd, valueType, ignoreDefaultValue);
        sb.append(valueCodeGenerator.codegen(value, String.format("%s.getValue()", entry)));
        sb.append(mapResult).append(".put(").append(key).append(", ").append(value).append(");");
        sb.append("}");
        sb.append(resultVariable).append(" = new GenericMapData(").append(mapResult).append(");");
        return sb.toString();
    }

}
