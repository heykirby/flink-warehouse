package com.sdu.streaming.frog.format;

import java.lang.reflect.Constructor;

public class ReflectionUtils {

    public static <T> T newInstance(Class<T> clazz) throws Exception {
        Constructor<T> constructor = clazz.getConstructor();
        return constructor.newInstance();
    }

}
