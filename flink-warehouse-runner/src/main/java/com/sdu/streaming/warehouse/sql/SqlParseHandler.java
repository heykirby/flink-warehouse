package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SqlParseHandler {

    TableMetadata parseTableMetadata(String name, Map<String, String> properties);

    List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink);

    StorageType supportedType();

}
