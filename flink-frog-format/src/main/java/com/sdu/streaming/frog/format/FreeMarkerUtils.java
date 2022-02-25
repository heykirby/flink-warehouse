package com.sdu.streaming.frog.format;

import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.flink.util.IOUtils;

import java.io.File;
import java.io.StringWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class FreeMarkerUtils {

    private static final Configuration cfg;

    static {
        cfg = new Configuration();
        // 设置模板目录
        URL templateURL = FreeMarkerUtils.class.getClassLoader().getResource("template");
        if (templateURL == null) {
            throw new RuntimeException("cant find 'template' directory.");
        }
        try {
            cfg.setDirectoryForTemplateLoading(new File(templateURL.getFile()));
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

//    public static void main(String[] args) {
//        Map<String, Object> props = new HashMap<>();
//        props.put("protobufClass", "com.sdu.Frog");
//        System.out.println(getTemplateCode("protobufRowConverter.ftl", props));
//    }

}
