package com.sdu.streaming.frog.format.protobuf;

import com.sdu.streaming.frog.utils.Base64Utils;
import com.sdu.streaming.frog.format.protobuf.ProtobufUtils;
import com.sdu.streaming.frog.format.RowDataTypeUtils;
import com.sdu.streaming.frog.format.RuntimeRowDataConverter;

//
import ${protobufClass};
import ${protobufClass}.*;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;


public class ProtobufRuntimeRowConverter implements RuntimeRowDataConverter {

    private static final String DEFAULT_PATH_TEMPLATE = "$.%s";

    private final RowType rowType;
    private final Map<String, String> fieldMappings;

    public ProtobufRuntimeRowConverter(RowType rowType, String fieldMapping) {
        this.rowType = rowType;
        this.fieldMappings = convertFieldMappings(fieldMapping);
    }

    @Override
    public RowData convert(byte[] bytes) throws Exception {
        ${protobufClass} msg = ${protobufClass}.parseFrom(bytes);
        GenericRowData rowData = new GenericRowData(rowType.getFieldNames().size());
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            String fieldPath = getFieldPath(fieldName, paths);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            Object fieldValue = getFieldValue(msg, subType, fieldPath);
            rowData.setField(index, fieldValue);
            index += 1;
        }
        return rowData;
    }

    private Object getFieldValue(${protobufClass} msg, LogicalType type, String path) {
        String[] fieldPaths = path.substring(2).split("\\.");
        // 判断是否链路是否为空
        boolean simpleType = RowDataTypeUtils.isBasicType(type);
        for (String fieldName : fieldPath) {

        }
    }

    private static Map<String, String> convertFieldMappings(String fieldMapping) {
        // 1. base64解码
        // 2. column1=path1;column2=path2;...
        try {
            if (fieldMapping == null || fieldMapping.isEmpty()) {
                return Collections.emptyMap();
            }
            Map<String, String> mappings = new HashMap<>();
            String path = Base64Utils.decode(fieldMapping);
            String[] paths = path.split(";");
            for (String p : paths) {
                String[] kv = p.split("=");
                mappings.put(kv[0], kv[1]);
            }
            return mappings;
        } catch (Throwable t) {
            throw new RuntimeException("failed deserialize field mapping", t);
        }
    }

    private static String getFieldPath(String fieldName, Map<String, String> paths) {
        if (paths == null || !paths.containsKey(fieldName)) {
            return String.format(DEFAULT_PATH_TEMPLATE, fieldName);
        }
        String path = paths.get(fieldName);
        if (path == null || path.isEmpty()) {
            return String.format(DEFAULT_PATH_TEMPLATE, fieldName);
        }
        return path;
    }
}