package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.Descriptors;

public class ProtobufUtils {

    public static final String OUTER_CLASS = "OuterClass";

    private ProtobufUtils() {

    }

    public static Descriptors.Descriptor getProtobufDescriptor(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            return (Descriptors.Descriptor) clazz.getMethod("getDescriptor").invoke(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String fieldNameToJsonName(String name) {
        final int length = name.length();
        StringBuilder result = new StringBuilder(length);
        boolean isNextUpperCase = false;
        for (int i = 0; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                isNextUpperCase = true;
            } else if (isNextUpperCase) {
                // This closely matches the logic for ASCII characters in:
                // http://google3/google/protobuf/descriptor.cc?l=249-251&rcl=228891689
                if ('a' <= ch && ch <= 'z') {
                    ch = (char) (ch - 'a' + 'A');
                    isNextUpperCase = false;
                }
                result.append(ch);
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    public static String getStrongCamelCaseJsonName(String name) {
        String jsonName = fieldNameToJsonName(name);
        if (jsonName.length() == 1) {
            return jsonName.toUpperCase();
        } else {
            return jsonName.substring(0, 1).toUpperCase() + jsonName.substring(1);
        }
    }

    public static String getJavaFullName(Descriptors.Descriptor descriptor) {
        String javaPackageName = descriptor.getFile().getOptions().getJavaPackage();
        if (descriptor.getFile().getOptions().getJavaMultipleFiles()) {
            //multiple_files=true
            if (null != descriptor.getContainingType()) {
                //nested type
                String parentJavaFullName = getJavaFullName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                //top level message
                return javaPackageName + "." + descriptor.getName();
            }
        } else {
            //multiple_files=false
            if (null != descriptor.getContainingType()) {
                //nested type
                String parentJavaFullName = getJavaFullName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                //top level message
                if (!descriptor.getFile().getOptions().hasJavaOuterClassname()) {
                    //user do not define outer class name in proto file
                    return javaPackageName + "." + descriptor.getName() + OUTER_CLASS + "." + descriptor.getName();
                } else {
                    String outerName = descriptor.getFile().getOptions().getJavaOuterClassname();
                    //user define outer class name in proto file
                    return javaPackageName + "." + outerName + "." + descriptor.getName();
                }
            }
        }
    }

    public static String getJavaType(Descriptors.FieldDescriptor fd) {
        switch (fd.getJavaType()) {
            case MESSAGE:
                return getJavaFullName(fd.getMessageType());
            case INT:
                return "Integer";
            case LONG:
                return "Long";
            case STRING:
            case ENUM:
                return "Object";
            case FLOAT:
                return "Float";
            case DOUBLE:
                return "Double";
            case BYTE_STRING:
                return "byte[]";
            case BOOLEAN:
                return "Boolean";
            default:
                throw new UnsupportedOperationException("unsupported field type: " + fd.getJavaType());
        }
    }
}
