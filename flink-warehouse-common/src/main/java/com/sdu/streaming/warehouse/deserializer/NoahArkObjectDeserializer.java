package com.sdu.streaming.warehouse.deserializer;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public interface NoahArkObjectDeserializer<T> extends Serializable {

    void serializer(T data, int pos, DataOutput out) throws IOException;

}
