package com.sdu.streaming.frog.format.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import static java.lang.String.format;

public class ProtobufUtils {

    public static Descriptors.Descriptor getProtobufDescriptor(String className, ClassLoader classLoader) {
        try {
            Class<?> clazz = Class.forName(className, true, classLoader);
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

    public static String getProtobufWrapperClass(Descriptors.Descriptor descriptor) {
        // see https://developers.google.com/protocol-buffers/docs/javatutorial
        DescriptorProtos.FileOptions options = descriptor.getFile().getOptions();
        String javaPackage = options.hasJavaPackage() ? options.getJavaPackage()
                : descriptor.getFile().getPackage();
        String wrapperClass = options.hasJavaOuterClassname() ? options.getJavaOuterClassname()
                : getProtobufWrapperClass(descriptor.getFile().toProto());
        return format("%s.%s", javaPackage, wrapperClass);
    }

    public static String getJavaFullName(Descriptors.Descriptor descriptor) {
        // see https://developers.google.com/protocol-buffers/docs/javatutorial
        // nb
        DescriptorProtos.FileOptions options = descriptor.getFile().getOptions();
        String javaPackage = options.hasJavaPackage() ? options.getJavaPackage()
                : descriptor.getFile().getPackage();
        if (options.hasJavaMultipleFiles()) {
            return format("%s.%s", javaPackage, descriptor.getName());
        }
        String wrapperClass = options.hasJavaOuterClassname() ? options.getJavaOuterClassname()
                : getProtobufWrapperClass(descriptor.getFile().toProto());
        return format("%s.%s.%s", javaPackage, wrapperClass, descriptor.getName());
    }

    private static String getProtobufWrapperClass(DescriptorProtos.FileDescriptorProto proto) {
        String name = proto.getName();
        int index = name.lastIndexOf("/");
        String fileName = index != -1 ? name.substring(index + 1) : name;
        String fileNameWithNoSuffix = fileName.split("\\.")[0];
        return getStrongCamelCaseJsonName(fileNameWithNoSuffix);
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
