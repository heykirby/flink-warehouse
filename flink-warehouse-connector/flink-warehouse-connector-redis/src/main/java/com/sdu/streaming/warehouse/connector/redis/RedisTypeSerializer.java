package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.GenericDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.GenericDataSerializer;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;

public interface RedisTypeSerializer<T> extends Serializable {

    byte[] serializeKey(RowData rowData, String prefix, RowData.FieldGetter[] keyFieldGetters, GenericDataSerializer[] rowKeySerializers) throws IOException;

    T serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, GenericDataSerializer[] rowFieldSerializers) throws IOException;

    RowData deserializeValue(T bytes, String[] fieldNames, GenericDataDeserializer[] rowFieldDeserializers) throws IOException;

}
