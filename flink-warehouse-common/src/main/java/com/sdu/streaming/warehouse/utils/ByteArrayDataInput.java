package com.sdu.streaming.warehouse.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public class ByteArrayDataInput implements DataInput {

    private final DataInput input;
//    private final NoahArkByteArrayInputStream stream;


    public ByteArrayDataInput(byte[] bytes) {
        this.input = new DataInputStream(new ByteArrayInputStream(bytes));
//        this.stream = new NoahArkByteArrayInputStream(bytes);
//        this.input = new DataInputStream(stream);
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.input.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        this.input.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return this.input.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return this.input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return this.input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return this.input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return this.input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return this.input.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return this.input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return this.input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return this.input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return this.input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return this.input.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        return this.input.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return this.input.readUTF();
    }

//    public void replace(byte[] bytes) {
//        this.stream.replace(bytes);
//    }
}
