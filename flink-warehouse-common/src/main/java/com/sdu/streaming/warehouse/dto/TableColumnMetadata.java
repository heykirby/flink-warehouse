package com.sdu.streaming.warehouse.dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class TableColumnMetadata implements Serializable {

    private String name;
    private String type;
    private boolean nullable;

}
