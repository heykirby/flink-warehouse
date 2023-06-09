package com.sdu.streaming.warehouse.sql;

import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;

import java.util.*;

import static com.sdu.streaming.warehouse.entry.TaskType.COMPUTE;
import static com.sdu.streaming.warehouse.entry.TaskType.SYNC;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

@Deprecated
public class KafkaTableSqlParseHandler extends AbstractSqlParseHandler {

    public static final KafkaTableSqlParseHandler INSTANCE = new KafkaTableSqlParseHandler();

    private static final String PROPERTY_KAFKA_SERVERS = "properties.bootstrap.servers";
    private static final String PROPERTY_KAFKA_TOPIC = "topic";
    private static final String PROPERTY_KAFKA_SYNC_TABLE = "properties.sync-table";


    private KafkaTableSqlParseHandler() { }

    @Override
    public TableMetadata parseTableMetadata(String name, Map<String, String> properties) {
        String servers = properties.get(PROPERTY_KAFKA_SERVERS);
        String topic = properties.get(PROPERTY_KAFKA_TOPIC);
        if (servers == null || servers.isEmpty()) {
            throw new RuntimeException("cant find 'properties.bootstrap.servers' property for table '" + name + "'");
        }
        if (topic == null || topic.isEmpty()) {
            throw new RuntimeException("cant find 'topic' property for table '" + name + "'");
        }
        return new TableMetadata(supportedType(), name, servers, topic, properties);
    }

    @Override
    public List<Lineage> createTableLineages(Set<TableMetadata> sources, TableMetadata sink) {
        // TODO: 若实时数仓链路, 链路节点是用kafka衔接, 则节点kafka不需输出到其他存储表
        // TODO: 当前实时数仓链路不存在中间链路

        String syncTable = sink.getProperties().get(PROPERTY_KAFKA_SYNC_TABLE);
        if (syncTable == null || syncTable.isEmpty()) {
            throw new RuntimeException("cant find 'properties.sync-table' property for table '" + sink.getName() + "'");
        }
        List<TableMetadata> syncTables = Arrays.stream(syncTable.split(";"))
                .map(table -> {
                    String[] tableMetadata = table.split("\\.");
                    if (tableMetadata.length != 3) {
                        throw new RuntimeException("kafka sync table format pattern should be 'xxx.xxx.xx'");
                    }
                    StorageType type = StorageType.fromName(tableMetadata[0]);
                    return new TableMetadata(type, tableMetadata[2], tableMetadata[1], tableMetadata[2], emptyMap());
                })
                .collect(toList());

        List<Lineage> lineages = new LinkedList<>();
        // 计算任务
        lineages.addAll(
                sources.stream()
                .map(source -> new Lineage(COMPUTE, source, sink))
                .collect(toList())
        );

        // 同步任务
        lineages.addAll(
                syncTables.stream()
                .map(target -> new Lineage(SYNC, sink, target))
                .collect(toList())
        );

        return lineages;
    }

    @Override
    public StorageType supportedType() {
        return StorageType.KAFKA;
    }

}
