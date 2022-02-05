package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Internal
public class ProtobufRowDataConverters implements Serializable {

    private final Map<String, String> fieldMappings;
    private final String timePattern;

    public ProtobufRowDataConverters(Map<String, String> fieldMappings,
                                     String timePattern) {
        this.fieldMappings = fieldMappings;
        this.timePattern = timePattern;
    }

    public interface RowDataConverter extends Serializable {

        Object convert(DynamicMessage msg, String[] paths, int start);

    }

    public RowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            // 基本类型
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case VARCHAR:
            case CHAR:
                return this::getColumnValue;
            // 复合类型
            case ARRAY:
                return createArrayConvert((ArrayType) type);
            case MAP:
            case ROW:
                return createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);

        }
    }

    @SuppressWarnings("unchecked")
    private RowDataConverter createArrayConvert(ArrayType arrayType) {
        RowDataConverter elementConvert = createConverter(arrayType.getElementType());
        final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        // ARRAY<t>
        // 1. if 't' is basic type
        // 2. if 't' is complex type, element extract path should be relative path
        return (value, paths, index) -> {
            Object columnValue = getColumnValue(value, paths, index);
            if (!(columnValue instanceof List)) {
                throw new RuntimeException("Illegal column type.");
            }
            if (TypeUtils.isBasicType(arrayType.getElementType())) {
                return columnValue;
            }
            throw new UnsupportedOperationException("unsupported nested complex type");
        };
    }

    private RowDataConverter createRowConverter(RowType rowType) {
        final RowDataConverter[] fieldConverters = rowType.getFields()
                .stream()
                .map(RowType.RowField::getType)
                .map(this::createConverter)
                .toArray(RowDataConverter[]::new);
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return (value, paths, index) -> {
            GenericRowData row = new GenericRowData(fieldNames.length);
            for (int i = 0; i < fieldNames.length; ++i) {
                String fieldPath = fieldMappings.get(fieldNames[i]);
                if (fieldPath == null || fieldPath.isEmpty()) {
                    throw new IllegalArgumentException("cant find field extract path, column: " + fieldNames[i]);
                }
                if (!fieldPath.startsWith("$")) {
                    throw new IllegalArgumentException("field extract path must start with '$', column: " + fieldNames[i]);
                }
                String[] fieldPaths = fieldPath.substring(1).split("\\.");
                RowDataConverter fieldConverter = fieldConverters[i];
                // name ROW<name1 type1, name2 type2>
                row.setField(i, fieldConverter.convert(value, fieldPaths, index));
            }
            return row;
        };
    }

    private Object getColumnValue(DynamicMessage msg, String[] paths, int start) {
        Descriptors.Descriptor desc = msg.getDescriptorForType();
        Object result = null;
        for (int i = start; i < paths.length; ++i) {
            Descriptors.FieldDescriptor fd = desc.findFieldByName(paths[i]);
            if (!msg.hasField(fd)) {
                return null;
            }
            if (i != paths.length - 1) {
                msg = (DynamicMessage) msg.getField(fd);
                desc = fd.getMessageType();
            } else {
                result = msg.getField(fd);
            }
        }
        return result;
    }

}
