package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.annotation.UserFunctionHint;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class UserFunctionDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(UserFunctionDiscovery.class);

    private static final String DEFAULT_FUNCTIONS_PACKAGE = "com.sdu.streaming.warehouse.functions";

    private UserFunctionDiscovery() {

    }

    @SuppressWarnings("unchecked")
    public static void registerBuildInUserFunction(TableEnvironment tableEnv) {
        ConfigurationBuilder configuration = new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(DEFAULT_FUNCTIONS_PACKAGE))
                .addScanners(new SubTypesScanner(), new MethodAnnotationsScanner(), new TypeAnnotationsScanner());
        Reflections reflections = new Reflections(configuration);
        Set<Class<?>> buildInFunctions = reflections.getTypesAnnotatedWith(UserFunctionHint.class);
        buildInFunctions.forEach(clazz -> {
            UserFunctionHint functionHint = clazz.getAnnotation(UserFunctionHint.class);
            LOG.info("register build in function, alias: {}, author: {}", functionHint.alias(), functionHint.author());
            tableEnv.createFunction(functionHint.alias(), (Class<? extends UserDefinedFunction>) clazz);
        });
    }

}
