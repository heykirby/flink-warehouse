package com.sdu.streaming.warehouse;

import com.sdu.streaming.warehouse.dto.WarehouseJob;
import com.sdu.streaming.warehouse.utils.Base64Utils;
import com.sdu.streaming.warehouse.utils.JsonUtils;
import com.sdu.streaming.warehouse.utils.ModifyOperationAnalyzer;
import com.sdu.streaming.warehouse.entry.WarehouseLineage;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.StatementSetImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static com.sdu.streaming.warehouse.utils.JsonUtils.toJson;
import static com.sdu.streaming.warehouse.utils.UserFunctionDiscovery.registerBuildInUserFunction;

public class WarehouseJobBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(WarehouseJobBootstrap.class);

    private static final String TASK_CONFIG_KEY = "taskConfig";
    private static final String DEFAULT_JOB_NAME = "Warehouse-Job";

    private static void checkWarehouseJob(WarehouseJob task) {
        if (task == null) {
            throw new IllegalArgumentException("undefine task");
        }
        if (task.getOptions() == null) {
            task.setOptions(Collections.emptyList());
        }
        if (task.getMaterials() == null || task.getMaterials().isEmpty()) {
            throw new IllegalArgumentException("undefine job materials");
        }
        if (task.getCalculates() == null || task.getCalculates().isEmpty()) {
            throw new IllegalArgumentException("undefine job execute logic");
        }
        if (task.getName() == null || task.getName().isEmpty()) {
            task.setName(DEFAULT_JOB_NAME);
        }
    }

    private static TableEnvironment createTableEnvironment(WarehouseJob warehouseJob) {
        return warehouseJob.isStreaming() ? createStreamTableEnvironment(warehouseJob)
                                          : createBatchTableEnvironment(warehouseJob);
    }

    private static TableEnvironment createStreamTableEnvironment(WarehouseJob warehouseJob) {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static TableEnvironment createBatchTableEnvironment(WarehouseJob warehouseJob) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        // TODO: load hive catalog
        //
        return tableEnv;
    }

    private static void appendJobConfiguration(final TableEnvironment tableEnv, WarehouseJob warehouseJob) {
        warehouseJob.getOptions().forEach(sql -> {
            if (tableEnv instanceof TableEnvironmentInternal) {
                TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
                List<Operation> operations = tableEnvInternal.getParser().parse(sql);
                if (operations == null || operations.isEmpty()) {
                    return;
                }
                Operation operation = operations.get(0);
                if (operation instanceof SetOperation) {
                    SetOperation set = (SetOperation) operation;
                    Configuration cfg = tableEnvInternal.getConfig().getConfiguration();
                    if (set.getKey().isPresent() && set.getValue().isPresent()) {
                        cfg.setString(set.getKey().get(), set.getValue().get());
                    }
                }
            }
        });
    }

    private static void executeJobMaterials(TableEnvironment tableEnv, WarehouseJob warehouseJob) {
        registerBuildInUserFunction(tableEnv);
        warehouseJob.getMaterials().forEach(tableEnv::executeSql);
    }

    private static StatementSet executeJobCalculateLogic(TableEnvironment tableEnv, WarehouseJob warehouseJob) throws Exception {
        StatementSet statement = tableEnv.createStatementSet();
        warehouseJob.getCalculates().forEach(statement::addInsertSql);
        buildJobLineage(statement, warehouseJob);
        return statement;
    }

    private static void buildJobLineage(StatementSet statement, WarehouseJob warehouseJob) throws Exception {
        Preconditions.checkArgument(statement instanceof StatementSetImpl);
        StatementSetImpl<?> statementSet = (StatementSetImpl<?>) statement;
        List<ModifyOperation> operations = statementSet.getOperations();
        // STEP1：解析血缘
        List<WarehouseLineage> lineages = ModifyOperationAnalyzer.analyze(warehouseJob.getName(), operations);
        // STEP2：上报血缘
        LOG.info("Task({}) lineage: {}", warehouseJob.getName(), toJson(lineages));
    }

    private static void executeWarehouseJob(TableEnvironment tableEnv, StatementSet statements, WarehouseJob task) {
        tableEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, task.getName());
        statements.execute();
    }

    public static void run(String[] args) {
        WarehouseJob warehouseJob = null;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String taskJson = Base64Utils.decode(parameterTool.get(TASK_CONFIG_KEY));
            warehouseJob = JsonUtils.fromJson(taskJson, WarehouseJob.class);
            // STEP1: 校验参数
            checkWarehouseJob(warehouseJob);
            // STEP2: 环境配置
            TableEnvironment tableEnv = createTableEnvironment(warehouseJob);
            // STEP3: 参数配置
            appendJobConfiguration(tableEnv, warehouseJob);
            // STEP4: 注册数据源、自定义函数
            executeJobMaterials(tableEnv, warehouseJob);
            // STEP5: 注册计算逻辑
            StatementSet statements = executeJobCalculateLogic(tableEnv, warehouseJob);
            // STEP6: 提交任务
            executeWarehouseJob(tableEnv, statements, warehouseJob);
        } catch (Exception e) {

            throw new RuntimeException("failed execute job", e);
        }
    }

    public static void main(String[] args) {
        WarehouseJobBootstrap.run(args);
    }

}
