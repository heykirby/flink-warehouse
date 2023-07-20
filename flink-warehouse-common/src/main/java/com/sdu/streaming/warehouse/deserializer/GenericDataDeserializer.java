package com.sdu.streaming.warehouse.deserializer;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

import static com.sdu.streaming.warehouse.deserializer.GenericDataDeserializerUtils.*;

public interface GenericDataDeserializer extends Serializable {

    Object deserializer(DataInput input) throws IOException;

    static GenericDataDeserializer createDataDeserializer(LogicalType fieldType) {
        GenericDataDeserializer deserializer;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                deserializer = GenericDataDeserializerUtils::deserializeStringData;
                break;
            case BOOLEAN:
                deserializer = GenericDataDeserializerUtils::deserializeBooleanData;
                break;
            case BINARY:
            case VARBINARY:
                deserializer = GenericDataDeserializerUtils::deserializeBinaryData;
                break;
            case DECIMAL:
                deserializer = GenericDataDeserializerUtils::deserializeDecimalData;
                break;
            case TINYINT:
                deserializer = GenericDataDeserializerUtils::deserializeByteData;
                break;
            case SMALLINT:
                deserializer = GenericDataDeserializerUtils::deserializeShortData;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                deserializer = GenericDataDeserializerUtils::deserializeIntData;
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                deserializer = GenericDataDeserializerUtils::deserializeLongData;
                break;
            case FLOAT:
                deserializer = GenericDataDeserializerUtils::deserializeFloatData;
                break;
            case DOUBLE:
                deserializer = GenericDataDeserializerUtils::deserializeDoubleData;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                deserializer = GenericDataDeserializerUtils::deserializeTimestampData;
                break;
            case RAW:
            case DISTINCT_TYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                ArrayType arrayType = (ArrayType) fieldType;
                deserializer = input -> deserializeArrayData(input, arrayType.getElementType());
                break;
            case MULTISET:
            case MAP:
                MapType mapType = (MapType) fieldType;
                deserializer = input -> deserializeMapData(input, mapType.getKeyType(), mapType.getValueType());
                break;
            case ROW:
            case STRUCTURED_TYPE:
                RowType rowType = (RowType) fieldType;
                deserializer = input -> deserializeRowData(input, rowType);
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }

        return deserializer;
    }


}
