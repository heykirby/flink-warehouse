package com.sdu.streaming.frog.format.protobuf;

import org.apache.flink.table.types.logical.LogicalType;

public class RowDataTypeUtils {

    private RowDataTypeUtils() {

    }

    public static boolean isBasicType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case VARCHAR:
            case CHAR:
                return true;
            default:
                return false;
        }
    }
}
