package com.sdu.streaming.frog.format;

import java.util.ArrayList;
import java.util.List;

public class CodegenContext {

    private List<String> methods;

    public CodegenContext() {
        this.methods = new ArrayList<>();
    }

    public void addMethod(String method) {
        this.methods.add(method);
    }
}
