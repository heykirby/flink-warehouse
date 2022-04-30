package com.sdu.streaming.frog.format.protobuf;

public interface TypeConverterCodeGenerator {

    String codegen(String resultVariable, String inputCode);

}
