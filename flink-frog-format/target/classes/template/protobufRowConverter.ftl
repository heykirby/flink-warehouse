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

    //
    private final RowType rowType;
    //
    private final Map<String, String> fieldMappings;

    public ProtobufRuntimeRowConverter(RowType rowType) {
        this.rowType = rowType;
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