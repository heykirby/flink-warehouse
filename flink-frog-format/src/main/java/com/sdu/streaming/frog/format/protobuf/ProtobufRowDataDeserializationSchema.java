package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import com.sdu.streaming.frog.format.FreeMarkerUtils;
import com.sdu.streaming.frog.format.ReflectionUtils;
import com.sdu.streaming.frog.format.RuntimeRowDataConverter;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.UserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.sdu.streaming.frog.format.protobuf.ProtobufTypeConverterFactory.getRowTypeConverterCodeGenerator;
import static com.sdu.streaming.frog.format.protobuf.ProtobufUtils.getProtobufDescriptor;
import static org.apache.flink.table.runtime.generated.CompileUtils.compile;

@Internal
public class ProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowDataDeserializationSchema.class);

    private static final String PROTOBUF_CODE_TEMPLATE_NAME = "ProtobufRuntimeRowDataConverter.ftl";
    private static final String PROTOBUF_CLASS_MACRO = "protobuf_class";
    private static final String PROTOBUF_INPUT_MACRO = "input_variable";
    private static final String PROTOBUF_INPUT_VAR_NAME = "message";
    private static final String PROTOBUF_OUTPUT_MACRO = "result_variable";
    private static final String PROTOBUF_OUTPUT_VAR_NAME = "row";
    private static final String PROTOBUF_CONVERT_MACRO = "converter_code";

    private static final String PROTOBUF_ROW_CONVERTER_CLASS = "com.sdu.streaming.frog.format.protobuf.ProtobufRuntimeRowDataConverter";

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;
    private final String clazz;
    private final boolean ignoreParserErrors;
    private final boolean ignoreDefaultValue;
    private final String fieldMapping;


    private transient RuntimeRowDataConverter runtimeRowDataConverter;

    public ProtobufRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            String clazz,
            String fieldMapping,
            boolean ignoreParserErrors,
            boolean ignoreDefaultValue) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.fieldMapping = fieldMapping;
        this.clazz = clazz;
        this.ignoreParserErrors = ignoreParserErrors;
        this.ignoreDefaultValue = ignoreDefaultValue;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        // STEP1: 数据读取映射
        Map<String, String[]> fieldMappings = standardFieldMappings(fieldMapping, rowType);
        // STEP2: 生成模板代码
        Map<String, Object> props = new HashMap<>();
        props.put(PROTOBUF_CLASS_MACRO, clazz);
        props.put(PROTOBUF_INPUT_MACRO, PROTOBUF_INPUT_VAR_NAME);
        props.put(PROTOBUF_OUTPUT_MACRO, PROTOBUF_OUTPUT_VAR_NAME);

        Descriptors.Descriptor descriptor = getProtobufDescriptor(clazz);
        TypeConverterCodeGenerator codeGenerator = getRowTypeConverterCodeGenerator(descriptor, rowType, fieldMappings, ignoreDefaultValue);
        props.put(PROTOBUF_CONVERT_MACRO, codeGenerator.codegen(PROTOBUF_OUTPUT_VAR_NAME, PROTOBUF_INPUT_VAR_NAME));

        String codegen = FreeMarkerUtils.getTemplateCode(PROTOBUF_CODE_TEMPLATE_NAME, props);
        LOG.info("codegen: \n {}", codegen);

        // STEP3: 构建实例
        UserCodeClassLoader userCodeClassLoader = context.getUserCodeClassLoader();
        Class<RuntimeRowDataConverter> rowDataConverterClazz = compile(userCodeClassLoader.asClassLoader(), PROTOBUF_ROW_CONVERTER_CLASS, codegen);
        this.runtimeRowDataConverter = ReflectionUtils.newInstance(rowDataConverterClazz);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            if (message == null) {
                return null;
            }
            return runtimeRowDataConverter.convert(message);
        } catch (Throwable t) {
            if (ignoreParserErrors) {
                return null;
            }
            LOG.error("failed deserialize protobuf bytes, class: " + clazz);
            throw new IOException("failed deserialize protobuf bytes", t);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return resultTypeInfo;
    }

    private static Map<String, String[]> standardFieldMappings(String fieldMapping, RowType rowType) {
        Map<String, String[]> mappings = new HashMap<>();
        if (fieldMapping == null || fieldMapping.isEmpty()) {
            rowType.getFieldNames().forEach((field) -> {
                mappings.put(field, new String[] {field});
            });
            return mappings;
        }
        // format: field1=path1;field1=path1
        String[] mapping = fieldMapping.split(";");
        for (String mp : mapping) {
            String[] nameToPath = mp.split("=");
            if (!nameToPath[1].startsWith("$.")) {
                throw new RuntimeException("field path should start with '$.'");
            }
            mappings.put(nameToPath[0], nameToPath[1].substring(2).split("\\."));
        }
        return mappings;
    }
}
