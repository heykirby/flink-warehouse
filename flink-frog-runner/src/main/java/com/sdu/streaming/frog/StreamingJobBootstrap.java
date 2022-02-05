package com.sdu.streaming.frog;

import com.sdu.streaming.frog.dto.FrogJobTask;
import com.sdu.streaming.frog.utils.Base64Utils;
import com.sdu.streaming.frog.utils.JsonUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;

import java.util.Collections;

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
        if (task.getCfg() == null) {
            throw new IllegalArgumentException("undefine job execute configuration");
        }
        if (task.getCfg().getOptions() == null) {
            task.getCfg().setOptions(Collections.emptyMap());
        }
        if (task.getCfg().getCheckpointCfg() == null) {
            // TODO: default checkpoint config
        }
        if (task.getName() == null || task.getName().isEmpty()) {
            task.setName(DEFAULT_JOB_NAME);
        }
    }

    private static void initializeCheckpointConfiguration(StreamExecutionEnvironment env, FrogJobTask task) {

    }

    private static TableEnvironment initializeTableEnvironment(FrogJobTask task) {
        return task.getCfg().isStreaming() ? initializeStreamTableEnvironment(task)
                                           : initializeBatchTableEnvironment(task);
    }

    private static TableEnvironment initializeStreamTableEnvironment(FrogJobTask task) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
        initializeJobConfiguration(tableEnv, task);
        return tableEnv;
    }

    private static TableEnvironment initializeBatchTableEnvironment(FrogJobTask task) {
        throw new UnsupportedOperationException("unsupported");
    }

    private static void initializeJobConfiguration(TableEnvironment tableEnv, FrogJobTask task) {
        if (task.getCfg().getOptions() == null) {
            return;
        }
        task.getCfg().getOptions().forEach((key, value) -> {
            if (value instanceof Integer) {
                tableEnv.getConfig().getConfiguration().setInteger(key, (Integer) value);
            } else if (value instanceof Double) {
                tableEnv.getConfig().getConfiguration().setDouble(key, (Double) value);
            } else if (value instanceof Long) {
                tableEnv.getConfig().getConfiguration().setLong(key, (Long) value);
            } else if (value instanceof Float) {
                tableEnv.getConfig().getConfiguration().setFloat(key, (Float) value);
            } else if (value instanceof String) {
                tableEnv.getConfig().getConfiguration().setString(key, (String) value);
            } else if (value instanceof Boolean) {
                tableEnv.getConfig().getConfiguration().setBoolean(key, (Boolean) value);
            } else {
                throw new IllegalArgumentException("unsupported configuration value type : " + value.getClass().getName());
            }
        });
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
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // STEP1: 校验参数
            checkStreamingJobParameters(task);
            // STEP2: 配置快照
            initializeCheckpointConfiguration(env, task);
            // STEP3: 环境配置
            TableEnvironment tableEnv = initializeTableEnvironment(task);
            // STEP4: 注册数据源、自定义函数
            initializeTaskMaterials(tableEnv, task);
            // STEP5: 注册任务计算逻辑
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
