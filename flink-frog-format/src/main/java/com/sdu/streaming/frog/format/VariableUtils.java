package com.sdu.streaming.frog.format;

import java.util.concurrent.atomic.AtomicInteger;

public class VariableUtils {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private VariableUtils() {

    }

    public static int getSerialId() {
        return ID.getAndIncrement();
    }

}
