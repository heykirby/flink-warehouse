package com.sdu.streaming.frog.utils;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class JsonUtils {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final ObjectMapper JSON_MAPPER;

    static {
        JSON_MAPPER = new ObjectMapper(JSON_FACTORY);
        JSON_MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    }

    private JsonUtils() {

    }

    public static String toJson(Object value) throws IOException {
        return JSON_MAPPER.writeValueAsString(value);
    }

    public static void toJson(OutputStream os, Object value) throws IOException {
        JSON_MAPPER.writeValue(os, value);
    }

    public static <T> T fromJson(String json, Class<T> clazz) throws IOException {
        return JSON_MAPPER.readValue(json, clazz);
    }

    public static <T> T fromJson(InputStream is, Class<T> clazz) throws IOException {
        return JSON_MAPPER.readValue(is, clazz);
    }

}
