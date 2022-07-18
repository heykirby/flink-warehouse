package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.List;
import java.util.Set;

import static com.sdu.streaming.warehouse.entry.TaskType.SYNC;
import static java.util.stream.Collectors.toList;

public abstract class AbstractSqlParseHandler implements SqlParseHandler {

    @Override
    public List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink) {
        return sources.stream()
                .map(source -> new Lineage(SYNC, source, sink))
                .collect(toList());
    }

}
