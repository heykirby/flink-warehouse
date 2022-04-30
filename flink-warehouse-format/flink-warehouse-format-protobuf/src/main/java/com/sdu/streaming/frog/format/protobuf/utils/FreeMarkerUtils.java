package com.sdu.streaming.frog.format.protobuf.utils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.flink.util.IOUtils;

import java.io.StringWriter;
import java.util.Map;

public class FreeMarkerUtils {

    private static final Configuration cfg;

    static {
        cfg = new Configuration();
        try {
            cfg.setClassForTemplateLoading(FreeMarkerUtils.class, "/template");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FreeMarkerUtils() {

    }

    public static String getTemplateCode(String fileName, Map<String, Object> properties) {
        StringWriter out = new StringWriter();
        try {
            Template template = cfg.getTemplate(fileName, "UTF-8");
            template.process(properties, out);
            return out.getBuffer().toString();
        } catch (Exception e) {
            throw new RuntimeException("failed generate template code", e);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

}
