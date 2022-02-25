package com.sdu.streaming.frog;

import com.sdu.streaming.frog.dto.FrogJobTask;
import com.sdu.streaming.frog.utils.Base64Utils;
import com.sdu.streaming.frog.utils.JsonUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

public class StreamingJobBootstrap {

    private static final String TASK_CONFIG_KEY = "taskConfig";
    private static final String DEFAULT_JOB_NAME = "frog";

    private static void checkStreamingJobParameters(FrogJobTask task) {
        if (task == null) {
            throw new IllegalArgumentException("undefine task");
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

    private static TableEnvironment initializeTableEnvironment(FrogJobTask task) {
        return task.isStreaming() ? initializeStreamTableEnvironment(task)
                                  : initializeBatchTableEnvironment(task);
    }

    private static TableEnvironment initializeStreamTableEnvironment(FrogJobTask task) {
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    private static TableEnvironment initializeBatchTableEnvironment(FrogJobTask task) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        // TODO: load hive catalog
        //
        return tableEnv;
    }

    private static void initializeTaskMaterials(TableEnvironment tableEnv, FrogJobTask task) {
        task.getMaterials().forEach(tableEnv::executeSql);
    }

    private static StatementSet initializeTaskCalculateLogic(TableEnvironment tableEnv, FrogJobTask task) {
        StatementSet statements = tableEnv.createStatementSet();
        task.getCalculates().forEach(statements::addInsertSql);
        return statements;
    }

    private static void initializeJobNameAndExecute(TableEnvironment tableEnv, StatementSet statements, FrogJobTask task) {
        tableEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, task.getName());
        statements.execute();
    }

    private static void run(String[] args) {
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String taskJson = Base64Utils.decode(parameterTool.get(TASK_CONFIG_KEY));
            FrogJobTask task = JsonUtils.fromJson(taskJson, FrogJobTask.class);
            // STEP1: 校验参数
            checkStreamingJobParameters(task);
            // STEP2: 环境配置
            TableEnvironment tableEnv = initializeTableEnvironment(task);
            // STEP3: 注册数据源、自定义函数
            initializeTaskMaterials(tableEnv, task);
            // STEP4: 注册任务计算逻辑
            StatementSet statements = initializeTaskCalculateLogic(tableEnv, task);
            // STEP5: 提交任务
            initializeJobNameAndExecute(tableEnv, statements, task);
        } catch (Exception e) {
            throw new RuntimeException("failed execute job", e);
        }
    }

    public static void main(String[] args) {
        StreamingJobBootstrap.run(args);
    }

}
