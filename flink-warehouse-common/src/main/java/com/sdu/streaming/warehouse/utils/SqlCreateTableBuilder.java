package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.dto.TableMetadata;

import java.util.HashMap;
import java.util.Map;

import static com.sdu.streaming.warehouse.utils.FreeMarkerUtils.getTemplateCode;

public class SqlCreateTableBuilder {

    private static final String TABLE_MACRO = "table";
    private static final String FREEMARKER_FILE = "CreateTable.ftl";


    private SqlCreateTableBuilder() { }

    public static String buildCreateTableStatement(TableMetadata tableMetadata) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(TABLE_MACRO, tableMetadata);
        return getTemplateCode(FREEMARKER_FILE, properties);
    }

}
