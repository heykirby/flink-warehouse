package com.sdu.streaming.warehouse.connector.redis;

import com.sdu.streaming.warehouse.deserializer.NoahArkDataDeserializer;
import com.sdu.streaming.warehouse.deserializer.NoahArkDataSerializer;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;

public interface RedisTypeSerializer<T> extends Serializable {

    byte[] serializeKey(RowData rowData, String prefix, RowData.FieldGetter[] keyFieldGetters, NoahArkDataSerializer[] rowKeySerializers) throws IOException;

    T serializeValue(RowData rowData, String[] fieldNames, RowData.FieldGetter[] rowFieldGetters, NoahArkDataSerializer[] rowFieldSerializers) throws IOException;

    RowData deserializeValue(T bytes, String[] fieldNames, NoahArkDataDeserializer[] rowFieldDeserializers) throws IOException;

}
