package com.sdu.streaming.warehouse.utils;

import java.io.ByteArrayInputStream;

public class NoahArkByteArrayInputStream extends ByteArrayInputStream {

    public NoahArkByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    public void replace(byte[] buf) {
        this.buf = buf;
        reset();
    }

}
