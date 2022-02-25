package com.sdu.streaming.frog.format.protobuf;

public class ProtobufUtils {

    private ProtobufUtils() {

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

}
