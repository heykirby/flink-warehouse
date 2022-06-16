package com.sdu.streaming.warehouse.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

// 参考: Guava ByteArrayDataInput
public class NoahArkByteArrayDataOutput implements DataOutput {

    final DataOutput output;
    final ByteArrayOutputStream byteArrayOutputStream;

    public NoahArkByteArrayDataOutput() {
        this(32);
    }

    public NoahArkByteArrayDataOutput(int size) {
        byteArrayOutputStream = new ByteArrayOutputStream(size);
        output = new DataOutputStream(byteArrayOutputStream);
    }

    @Override
    public void write(int b) throws IOException {
        this.output.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.output.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.output.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.output.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.output.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.output.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.output.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.output.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.output.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.output.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.output.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        this.output.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        this.output.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        this.output.writeUTF(s);
    }

    public byte[] toByteArray() {
        return this.byteArrayOutputStream.toByteArray();
    }

    public void reset() {
        this.byteArrayOutputStream.reset();
    }
}
