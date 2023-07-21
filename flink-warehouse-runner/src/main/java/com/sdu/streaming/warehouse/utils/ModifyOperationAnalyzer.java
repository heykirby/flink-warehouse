package com.sdu.streaming.warehouse.utils;

import static java.lang.String.format;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelEmptyColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdu.streaming.warehouse.entry.WarehouseLineage;

public enum ModifyOperationAnalyzer {

    CreateTableAsOperationAnalyzer() {

        @Override
        public boolean accept(ModifyOperation modifyOperation) {
            return modifyOperation instanceof CreateTableASOperation;
        }

        @Override
        public WarehouseLineage analyze(String jobName, TableEnvironment tableEnv, ModifyOperation modifyOperation) {
            CreateTableASOperation createTableASOperation = (CreateTableASOperation) modifyOperation;
            return null;
        }

    },

    SinkModifyOperationAnalyzer() {

        @Override
        public boolean accept(ModifyOperation modifyOperation) {
            return modifyOperation instanceof SinkModifyOperation;
        }

        @Override
        public WarehouseLineage analyze(String jobName, TableEnvironment tableEnv, ModifyOperation modifyOperation) {
            WarehouseLineage lineage = new WarehouseLineage(jobName);
            // INSERT INTO :
            //                   translate
            //  QueryOperation -------------> PlannerQueryOperation
            SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) modifyOperation;
            // target
            ContextResolvedTable resolvedTable = sinkModifyOperation.getContextResolvedTable();
            ObjectIdentifier objectIdentifier = resolvedTable.getIdentifier();
            ResolvedSchema resolvedSchema = resolvedTable.getResolvedSchema();
            // source
            PlannerQueryOperation plannerQueryOperation = (PlannerQueryOperation) sinkModifyOperation.getChild();
            RelNode queryRelNode = plannerQueryOperation.getCalciteTree();

            // build lineage
            Set<String> sourceTableNames = new HashSet<>();
            Map<String, Map<String, String>> sourceTableOptions = new HashMap<>();
            for (int i = 0; i < resolvedSchema.getColumnCount(); ++i) {
                String targetColumn = resolvedSchema.getColumnNames().get(i);
                RelMetadataQuery metadataQuery = queryRelNode.getCluster().getMetadataQuery();
                Set<RelColumnOrigin> sourceColumns =  metadataQuery.getColumnOrigins(queryRelNode, i);
                if (sourceColumns == null || sourceColumns.isEmpty()) {
                    throw new IllegalStateException("cant find source column for target column '" + targetColumn + "'");
                }
                for (RelColumnOrigin origin : sourceColumns) {
                    if (origin instanceof RelEmptyColumnOrigin) {
                        // implied constant
                        lineage.addTableColumnLineage(
                                "literal",
                                format("%s.%s", objectIdentifier.asSummaryString(), targetColumn));
                        continue;
                    }
                    RelOptTable table = origin.getOriginTable();
                    ObjectIdentifier tableIdentifier = from(table.getQualifiedName().toArray(new String[0]));
                    tableEnv.getCatalog(tableIdentifier.getCatalogName())
                            .ifPresent(catalog -> {
                                try {
                                    ObjectPath path = new ObjectPath(tableIdentifier.getDatabaseName(), tableIdentifier.getObjectName());
                                    CatalogBaseTable catalogTable = catalog.getTable(path);
                                    sourceTableOptions.put(tableIdentifier.asSummaryString(), catalogTable.getOptions());
                                    sourceTableNames.add(tableIdentifier.asSummaryString());
                                } catch (Exception e) {
                                    LOG.error("cant find table from catalog, name: {}", objectIdentifier.asSummaryString());
                                }

                            });
                    // column lineage
                    int ordinal = origin.getOriginColumnOrdinal();
                    String sourceColumnName = table.getRowType().getFieldNames().get(ordinal);
                    lineage.addTableColumnLineage(
                            format("%s.%s", tableIdentifier.asSummaryString(), sourceColumnName),
                            format("%s.%s", objectIdentifier.asSummaryString(), targetColumn));
                }
            }
            // table lineage
            lineage.buildTableLineage(
                    objectIdentifier.asSummaryString(),
                    resolvedTable.getTable().getOptions(),
                    sourceTableNames,
                    sourceTableOptions);

            return lineage;
        }

    };


    public abstract boolean accept(ModifyOperation modifyOperation);

    public abstract WarehouseLineage analyze(String jobName, TableEnvironment tableEnv, ModifyOperation modifyOperation);

    private static final Logger LOG = LoggerFactory.getLogger(ModifyOperationAnalyzer.class);

    private static ObjectIdentifier from(String[] names) {
        Preconditions.checkArgument(names.length == 3);
        return ObjectIdentifier.of(names[0], names[1], names[2]);
    }

    public static List<WarehouseLineage> analyze(String jobName, TableEnvironment tableEnv, List<ModifyOperation> modifyOperations) {
        if (modifyOperations == null || modifyOperations.isEmpty()) {
            return Collections.emptyList();
        }

        final List<WarehouseLineage> lineages = new LinkedList<>();
        modifyOperations.forEach(modifyOperation -> {
            boolean accepted = false;
            ModifyOperationAnalyzer[] analyzers = ModifyOperationAnalyzer.values();
            for (ModifyOperationAnalyzer analyzer : analyzers) {
                accepted = analyzer.accept(modifyOperation);
                if (accepted) {
                    lineages.add(analyzer.analyze(jobName, tableEnv, modifyOperation));
                }
            }
            if (!accepted) {
                LOG.warn("cant create lineage for '{}'", modifyOperation.getClass().getName());
            }
        });

        return lineages;
    }
}
