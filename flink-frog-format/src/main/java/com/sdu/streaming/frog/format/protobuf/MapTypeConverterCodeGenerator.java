package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import java.util.Map;

import static com.sdu.streaming.frog.format.VariableUtils.getSerialId;
import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getProtobufTypeConverterCodeGenerator;
import static java.lang.String.format;

public class MapTypeConverterCodeGenerator implements TypeConverterCodeGenerator {

    private final Descriptors.FieldDescriptor fd;
    private final LogicalType keyType;
    private final LogicalType valueType;
    private final Map<String, String[]> fieldMappings;
    private final boolean ignoreDefaultValue;

    public MapTypeConverterCodeGenerator(Descriptors.FieldDescriptor fd, MapType type, Map<String, String[]> fieldMappings, boolean ignoreDefaultValue) {
        this.fd = fd;
        this.keyType = type.getKeyType();
        this.valueType = type.getValueType();
        this.fieldMappings = fieldMappings;
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
        String input = format("input$%d", getSerialId());
        String keyType = ProtobufUtils.getJavaType(keyFd);
        String valueType = ProtobufUtils.getJavaType(valueFd);
        sb.append(format("Map<%s, %s> %s = %s;", keyType, valueType, input, inputCode));
        String ret = format("ret$%d", getSerialId());
        sb.append(format("Map<Object, Object> %s = new HashMap<>();", ret));
        String entry = format("entry$%d", getSerialId());
        sb.append(format("for(Map.Entry<%s, %s> %s : %s.entrySet()) { ", keyType, valueType, entry, input));
        String key = format("key$%d", getSerialId());
        String value = format("value$%d", getSerialId());
        sb.append(format("Object %s = null;", key));
        sb.append(format("Object %s = null;", value));
        TypeConverterCodeGenerator keyCodeGenerator = getProtobufTypeConverterCodeGenerator(fieldMappings, keyFd, this.keyType, ignoreDefaultValue);
        sb.append(keyCodeGenerator.codegen(key, format("%s.getKey()", entry)));
        TypeConverterCodeGenerator valueCodeGenerator = getProtobufTypeConverterCodeGenerator(fieldMappings, valueFd, this.valueType, ignoreDefaultValue);
        sb.append(valueCodeGenerator.codegen(value, format("%s.getValue()", entry)));
        sb.append(format("%s.put(%s, %s); }", ret, key, value));
        sb.append(format("%s = new GenericMapData(%s);", resultVariable, ret));
        return sb.toString();
    }

}
