package com.sdu.streaming.warehouse.format.protobuf;

public interface TypeConverterCodeGenerator {

    String codegen(String resultVariable, String inputCode);

}
