package com.sdu.streaming.warehouse.format.protobuf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

@Internal
public interface RuntimeRowDataConverter extends Serializable {

    RowData convert(byte[] bytes) throws Exception;

}
