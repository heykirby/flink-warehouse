package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.entry.WarehouseLineage;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public enum ModifyOperationAnalyzer {

    CreateTableASOperationAnalyzer() {

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
            Map<String, Map<String, String>> sourceTableFullNames = new HashMap<>();
            for (int i = 0; i < resolvedSchema.getColumnCount(); ++i) {
                String targetColumn = resolvedSchema.getColumnNames().get(i);
                RelMetadataQuery metadataQuery = queryRelNode.getCluster().getMetadataQuery();
                Set<RelColumnOrigin> sourceColumns =  metadataQuery.getColumnOrigins(queryRelNode, i);
                if (sourceColumns == null || sourceColumns.isEmpty()) {
                    throw new IllegalStateException("cant find source column for target column '" + targetColumn + "'");
                }
                for (RelColumnOrigin origin : sourceColumns) {
                    RelOptTable table = origin.getOriginTable();
                    ObjectIdentifier tableIdentifier = from(table.getQualifiedName().toArray(new String[0]));
                    tableEnv.getCatalog(tableIdentifier.getCatalogName())
                            .ifPresent(catalog -> {
                                try {
                                    ObjectPath path = new ObjectPath(tableIdentifier.getDatabaseName(), tableIdentifier.getObjectName());
                                    CatalogBaseTable catalogTable = catalog.getTable(path);
                                    sourceTableFullNames.put(tableIdentifier.asSummaryString(), catalogTable.getOptions());
                                } catch (Exception e) {
                                    LOG.error("cant find table from catalog, name: {}", objectIdentifier.asSummaryString());
                                }

                            });
                    // column lineage
                    int ordinal = origin.getOriginColumnOrdinal();
                    String sourceColumnName = table.getRowType().getFieldNames().get(ordinal);
                    lineage.addTableColumnLineage(sourceColumnName, targetColumn);
                }
            }
            // table lineage
            lineage.buildTableLineage(
                    sourceTableFullNames,
                    objectIdentifier.asSummaryString(),
                    resolvedTable.getTable().getOptions());

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
