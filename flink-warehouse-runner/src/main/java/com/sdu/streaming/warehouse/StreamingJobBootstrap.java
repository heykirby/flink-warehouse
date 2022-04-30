package com.sdu.streaming.warehouse;

import com.sdu.streaming.warehouse.dto.NoahArkJobTask;
import com.sdu.streaming.warehouse.utils.Base64Utils;
import com.sdu.streaming.warehouse.utils.JsonUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.Collections;
import java.util.List;

public class StreamingJobBootstrap {

    private static final String TASK_CONFIG_KEY = "taskConfig";
    private static final String DEFAULT_JOB_NAME = "NoahArk-Streaming-Job";

    private static void checkStreamingJobParameters(NoahArkJobTask task) {
        if (task == null) {
            throw new IllegalArgumentException("undefine task");
        }
        if (task.getConfigurations() == null) {
            task.setConfigurations(Collections.emptyList());
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

    private static TableEnvironment initializeTableEnvironment(NoahArkJobTask task) {
        return task.isStreaming() ? initializeStreamTableEnvironment(task)
                                  : initializeBatchTableEnvironment(task);
    }

    private static TableEnvironment initializeStreamTableEnvironment(NoahArkJobTask task) {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static TableEnvironment initializeBatchTableEnvironment(NoahArkJobTask task) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        // TODO: load hive catalog
        //
        return tableEnv;
    }

    private static void initializeTaskConfiguration(final TableEnvironment tableEnv, NoahArkJobTask task) {
        task.getConfigurations().forEach(sql -> {
            if (tableEnv instanceof TableEnvironmentImpl) {
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

    private static void initializeTaskMaterials(TableEnvironment tableEnv, NoahArkJobTask task) {
        task.getMaterials().forEach(tableEnv::executeSql);
    }

    private static StatementSet initializeTaskCalculateLogic(TableEnvironment tableEnv, NoahArkJobTask task) {
        StatementSet statements = tableEnv.createStatementSet();
        task.getCalculates().forEach(statements::addInsertSql);
        return statements;
    }

    private static void initializeJobNameAndExecute(TableEnvironment tableEnv, StatementSet statements, NoahArkJobTask task) {
        tableEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, task.getName());
        statements.execute();
    }

    private static void run(String[] args) {
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String taskJson = Base64Utils.decode(parameterTool.get(TASK_CONFIG_KEY));
            NoahArkJobTask task = JsonUtils.fromJson(taskJson, NoahArkJobTask.class);
            // STEP1: 校验参数
            checkStreamingJobParameters(task);
            // STEP2: 环境配置
            TableEnvironment tableEnv = initializeTableEnvironment(task);
            // STEP3: 参数配置
            initializeTaskConfiguration(tableEnv, task);
            // STEP4: 注册数据源、自定义函数
            initializeTaskMaterials(tableEnv, task);
            // STEP5: 注册计算逻辑
            StatementSet statements = initializeTaskCalculateLogic(tableEnv, task);
            // STEP6: 提交任务
            initializeJobNameAndExecute(tableEnv, statements, task);
        } catch (Exception e) {
            throw new RuntimeException("failed execute job", e);
        }
    }

    public static void main(String[] args) {
        StreamingJobBootstrap.run(args);
    }

}
