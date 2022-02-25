package com.sdu.streaming.frog.format.protobuf;

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

import static org.apache.flink.table.runtime.generated.CompileUtils.compile;

@Internal
public class ProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowDataDeserializationSchema.class);

    private static final String PROTOBUF_CODE_TEMPLATE_NAME = "protobufRowConverter.ftl";
    private static final String PROTOBUF_CLASS_MACRO_VARIABLES = "protobufClass";
    private static final String PROTOBUF_ROW_CONVERTER_CLASS = "com.sdu.streaming.frog.format.protobuf.ProtobufRuntimeRowConverter";

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
        // STEP1: 根据模板生成代码
        Map<String, Object> props = new HashMap<>();
        props.put(PROTOBUF_CLASS_MACRO_VARIABLES, clazz);
        String code = FreeMarkerUtils.getTemplateCode(PROTOBUF_CODE_TEMPLATE_NAME, props);
        LOG.info("codegen: \n {}", code);

        // STEP2: 构建实例
        UserCodeClassLoader userCodeClassLoader = context.getUserCodeClassLoader();
        Class<RuntimeRowDataConverter> rowDataConverterClazz = compile(userCodeClassLoader.asClassLoader(), PROTOBUF_ROW_CONVERTER_CLASS, code);
        Class<?>[] parameterTypes = new Class<?>[] {RowType.class, String.class};
        Object[] parameters = new Object[] {this.rowType, this.fieldMapping};
        this.runtimeRowDataConverter = ReflectionUtils.newInstance(rowDataConverterClazz, parameterTypes, parameters);
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

}
