package com.sdu.streaming.warehouse.dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Builder
public class TableMetadata implements Serializable {

    private String name;
    private List<TableColumnMetadata> columns;
    private String primaryKeys;
    private TableWatermarkMetadata watermark;
    private Map<String, String> properties;


}
