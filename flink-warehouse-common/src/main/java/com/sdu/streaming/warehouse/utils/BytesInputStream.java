package com.sdu.streaming.warehouse.utils;

import java.io.ByteArrayInputStream;

public class BytesInputStream extends ByteArrayInputStream {

    public BytesInputStream(byte[] buf) {
        super(buf);
    }

    public void replace(byte[] buf) {
        this.buf = buf;
        reset();
    }

}
