package com.sdu.streaming.warehouse.utils;

import com.sdu.warehouse.api.lineage.Lineage;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public enum ModifyOperationAnalyzer {

    CreateTableASOperationAnalyzer() {

        @Override
        public boolean accept(ModifyOperation modifyOperation) {
            return modifyOperation instanceof CreateTableASOperation;
        }

        @Override
        public Lineage analyze(String jobName, ModifyOperation modifyOperation) {
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
        public Lineage analyze(String jobName, ModifyOperation modifyOperation) {
            Lineage lineage = new Lineage(jobName);
            SinkModifyOperation sinkModifyOperation = (SinkModifyOperation) modifyOperation;
            // target
            ContextResolvedTable resolvedTable = sinkModifyOperation.getContextResolvedTable();
            // source
            QueryOperation queryOperation = sinkModifyOperation.getChild();
            resolvedTable.getResolvedSchema().getColumns()
                            .forEach(column -> {
                                String targetColumn = column.getName();

                            });
            lineage.buildTableLineage(
                    null,
                    String.join(".", resolvedTable.getIdentifier().getDatabaseName(), resolvedTable.getIdentifier().getObjectName()),
                    resolvedTable.getTable().getOptions()
                    );

            return lineage;
        }

    };


    public abstract boolean accept(ModifyOperation modifyOperation);

    public abstract Lineage analyze(String jobName, ModifyOperation modifyOperation);

    private static final Logger LOG = LoggerFactory.getLogger(ModifyOperationAnalyzer.class);

    public static List<Lineage> analyze(String jobName, List<ModifyOperation> modifyOperations) {
        if (modifyOperations == null || modifyOperations.isEmpty()) {
            return Collections.emptyList();
        }

        final List<Lineage> lineages = new LinkedList<>();
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
