package com.sdu.streaming.warehouse.format.protobuf.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class VariableUtils {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private VariableUtils() {

    }

    public static int getSerialId() {
        return ID.getAndIncrement();
    }

}
