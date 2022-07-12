package com.sdu.streaming.warehouse.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class VariableUtils {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private VariableUtils() {

    }

    public static int getSerialId() {
        return ID.getAndIncrement();
    }

}
