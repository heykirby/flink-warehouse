package com.sdu.streaming.warehouse.format.protobuf;

import com.sdu.streaming.warehouse.format.protobuf.RuntimeRowDataConverter;
import ${protobuf_class};
import ${protobuf_class}.*;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.StringData;

import java.lang.Integer;
import java.lang.Long;
import java.lang.Float;
import java.lang.Double;
import java.lang.Boolean;
import java.util.ArrayList;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class ProtobufRuntimeRowDataConverter implements RuntimeRowDataConverter {

    @Override
    public RowData convert(byte[] bytes) throws Exception {
        ${protobuf_class} ${input_variable} = ${protobuf_class}.parseFrom(bytes);
        return convert(${input_variable});
    }

    private RowData convert(${protobuf_class} ${input_variable}) throws Exception {
        RowData ${result_variable} = null;
        ${converter_code}
        return ${result_variable};
    }

}