package com.sdu.streaming.warehouse.deserializer;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import static com.sdu.streaming.warehouse.deserializer.GenericDataDeserializerUtils.*;

public interface GenericDataSerializer extends Serializable {

    void serializer(Object data, DataOutput out) throws IOException;

    static GenericDataSerializer createDataSerializer(LogicalType fieldType) {
        GenericDataSerializer serializer;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                serializer = (data, out) -> serializeStringData((StringData) data, out);
                break;
            case BOOLEAN:
                serializer = (data, out) -> serializeBooleanData((boolean) data, out);;
                break;
            case BINARY:
            case VARBINARY:
                serializer = (data, out) -> serializeBinaryData((byte[]) data, out);
                break;
            case DECIMAL:
                serializer = (data, out) ->  serializeDecimalData((DecimalData) data, out);
                break;
            case TINYINT:
                serializer = (data, out) -> serializeByteData((byte) data, out);;
                break;
            case SMALLINT:
                serializer = (data, out) -> serializeShortData((short) data, out);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                serializer = (data, out) -> serializeIntData((int) data, out);
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                serializer = (data, out) ->  serializeLongData((long) data, out);
                break;
            case FLOAT:
                serializer = (data, out) -> serializeFloatData((float) data, out);;
                break;
            case DOUBLE:
                serializer = (data, out) ->  serializeDoubleData((double) data, out);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                serializer = (data, out) -> serializeTimestampData((TimestampData) data, out);;
                break;
            case RAW:
            case DISTINCT_TYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                serializer = (data, out) -> serializeArrayData((ArrayData) data, arrayType.getElementType(), out);
                break;
            case MULTISET:
            case MAP:
                MapType type = (MapType) fieldType;
                serializer = (data, out) -> serializeMapData((MapData) data, type.getKeyType(), type.getValueType(), out);
                break;
            case ROW:
            case STRUCTURED_TYPE:
                RowType rowType = (RowType) fieldType;
                serializer = (data, out) -> serializeRowData((RowData) data, rowType, out);;
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }

        if (!fieldType.isNullable()) {
            return serializer;
        }
        return (data, out) -> {
            if (data == null) {
                return;
            }
            serializer.serializer(data, out);
        };
    }
}
