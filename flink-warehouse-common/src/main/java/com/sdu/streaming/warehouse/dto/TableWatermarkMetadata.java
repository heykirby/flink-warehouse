package com.sdu.streaming.warehouse.dto;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
@Builder
public class TableWatermarkMetadata implements Serializable {

    private String eventTimeColumn;
    private String strategy;

}
