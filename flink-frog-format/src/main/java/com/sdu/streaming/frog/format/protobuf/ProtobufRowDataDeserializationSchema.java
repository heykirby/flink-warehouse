package com.sdu.streaming.frog.format.protobuf;

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
import java.util.Map;

@Internal
public class ProtobufRowDataDeserializationSchema implements DeserializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufRowDataDeserializationSchema.class);

    private final RowType rowType;
    private final TypeInformation<RowData> resultTypeInfo;
    private final String clazz;
    private final boolean ignoreParserErrors;
    private final boolean ignoreDefaultValue;
    private final Map<String, String> fieldMappings;


    private transient RuntimeRowDataConverter runtimeRowDataConverter;

    public ProtobufRowDataDeserializationSchema(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            String clazz,
            Map<String, String> fieldMappings,
            boolean ignoreParserErrors,
            boolean ignoreDefaultValue) {
        this.rowType = rowType;
        this.resultTypeInfo = resultTypeInfo;
        this.fieldMappings = fieldMappings;
        this.clazz = clazz;
        this.ignoreParserErrors = ignoreParserErrors;
        this.ignoreDefaultValue = ignoreDefaultValue;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        UserCodeClassLoader userCodeClassLoader = context.getUserCodeClassLoader();
        // TODO: load and initialize
        this.runtimeRowDataConverter = null;
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
