package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.entry.WarehouseLineage;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
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
        public WarehouseLineage analyze(String jobName, ModifyOperation modifyOperation) {
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
        public WarehouseLineage analyze(String jobName, ModifyOperation modifyOperation) {
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
                    // TODO: 2023/6/8
                    RelOptTable sourceTable = origin.getOriginTable();

                    // column lineage
                    int ordinal = origin.getOriginColumnOrdinal();
                    String sourceColumnName = sourceTable.getRowType().getFieldNames().get(ordinal);
                    lineage.addTableColumnLineage(sourceColumnName, targetColumn);
                }
            }
            // table lineage
            lineage.buildTableLineage(
                    sourceTableFullNames,
                    String.join(".", objectIdentifier.getDatabaseName(), objectIdentifier.getObjectName()),
                    resolvedTable.getTable().getOptions());

            return lineage;
        }

    };


    public abstract boolean accept(ModifyOperation modifyOperation);

    public abstract WarehouseLineage analyze(String jobName, ModifyOperation modifyOperation);

    private static final Logger LOG = LoggerFactory.getLogger(ModifyOperationAnalyzer.class);

    public static List<WarehouseLineage> analyze(String jobName, List<ModifyOperation> modifyOperations) {
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
                    lineages.add(analyzer.analyze(jobName, modifyOperation));
                }
            }
            if (!accepted) {
                LOG.warn("cant create lineage for '{}'", modifyOperation.getClass().getName());
            }
        });

        return lineages;
    }
}
