package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Internal
public class ProtobufRowDataDeserialization implements DeserializationSchema<RowData> {

    private final TypeInformation<RowData> resultTypeInfo;

    private final Map<String, String> fieldMappings;

    private final Descriptors.Descriptor descriptor;

    private final String timePattern;

    private final boolean ignoreParserErrors;

    private final RowDataConverter runtimeConverter;

    public ProtobufRowDataDeserialization(
            RowType rowType,
            TypeInformation<RowData> resultTypeInfo,
            Map<String, String> fieldMappings,
            String clazz,
            String timePattern,
            boolean ignoreParserErrors) {
        this.resultTypeInfo = resultTypeInfo;
        this.fieldMappings = fieldMappings;
        this.clazz = clazz;
        this.timePattern = timePattern;
        this.ignoreParserErrors = ignoreParserErrors;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            DynamicMessage msg = DynamicMessage.parseFrom(descriptor, message);
        } catch (Throwable t) {
            if (ignoreParserErrors) {
                return null;
            }
            throw new IOException("failed to deserialize message", t);
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

    private interface RowDataConverter extends Serializable {

    }

}
