package com.sdu.streaming.frog.format;

import java.lang.reflect.Constructor;

public class ReflectionUtils {

    public static <T> T newInstance(Class<T> clazz, Class<?>[] parameterTypes, Object... parameters) throws Exception {
        Constructor<T> constructor = clazz.getConstructor(parameterTypes);
        return constructor.newInstance(parameters);
    }

}
