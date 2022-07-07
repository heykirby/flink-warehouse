package com.sdu.streaming.warehouse.utils;

import com.sdu.streaming.warehouse.dto.WarehouseJobTask;
import com.sdu.streaming.warehouse.entry.Lineage;
import com.sdu.streaming.warehouse.entry.StorageType;
import com.sdu.streaming.warehouse.entry.TableMetadata;
import com.sdu.streaming.warehouse.entry.TaskLineage;
import com.sdu.streaming.warehouse.sql.*;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

import java.util.*;

import static com.sdu.streaming.warehouse.entry.StorageType.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.join;

public class SqlParseUtils {

    private static final SqlConformance DEFAULT_SQL_CONFORMANCE = FlinkSqlConformance.DEFAULT;

    private static final Map<StorageType, SqlParseHandler> HANDLERS;

    static {
        HANDLERS = new HashMap<>();
        HANDLERS.put(KAFKA, KafkaTableSqlParseHandler.INSTANCE);
        HANDLERS.put(MYSQL, MySQLTableSqlParseHandler.INSTANCE);
        HANDLERS.put(HUDI, HudiTableSqlParseHandler.INSTANCE);
        HANDLERS.put(GENERIC, GenericTableSqlParseHandler.INSTANCE);
    }

    private SqlParseUtils() {

    }


    public static TaskLineage parseSql(WarehouseJobTask task) throws SqlParseException {
        // STEP1: 解析SQL
        SqlParser parser = SqlParser.create(combineSql(task), getSqlParserConfig());
        SqlNodeList sqlNodes = parser.parseStmtList();
        // STEP2: 数据表
        Map<String, TableMetadata> tableInfos = extractTableInfo(sqlNodes);
        // STEP3: 数据表血缘
        List<Lineage> lineages = extractTableLineage(tableInfos, sqlNodes);
        return new TaskLineage(task.getName(), lineages);
    }

    private static Map<String, TableMetadata> extractTableInfo(SqlNodeList sqlNodes) {
        final Map<String, TableMetadata> storageTypes = new HashMap<>();
        sqlNodes.forEach(sqlNode -> {
            if (sqlNode.getKind() == SqlKind.CREATE_TABLE) {
                SqlCreateTable createTable = (SqlCreateTable) sqlNode;
                SqlIdentifier tableName = createTable.getTableName();
                Map<String, String> properties = createTable.getPropertyList()
                        .getList()
                        .stream()
                        .filter(node -> node instanceof SqlTableOption)
                        .map(node -> (SqlTableOption) node)
                        .collect(toMap(SqlTableOption::getKeyString, SqlTableOption::getValueString));
                StorageType type = StorageType.fromProperties(properties);
                SqlParseHandler handler = HANDLERS.get(type);
                if (handler == null) {
                    throw new RuntimeException("cant find 'SqlParseHandler' for '" + type.getType() + "'");
                }
                TableMetadata metadata = handler.parseTableMetadata(join(tableName.names, '.'), properties);
                // 这里使用定义的逻辑表名
                storageTypes.put(metadata.getName(), metadata);
            }
        });
        return storageTypes;
    }

    private static List<Lineage> extractTableLineage(Map<String, TableMetadata> tables, SqlNodeList sqlNodes) throws SqlParseException {
        List<RichSqlInsert> inserts = sqlNodes.getList()
                .stream()
                .filter(sqlNode -> sqlNode.getKind() == SqlKind.INSERT)
                .map(sqlNode -> (RichSqlInsert) sqlNode)
                .collect(toList());
        if (inserts.isEmpty()) {
            throw new RuntimeException("cant find 'INSERT INTO xxx' statement");
        }
        List<Lineage> lineages = new LinkedList<>();
        for (RichSqlInsert insert : inserts) {
            TableMetadata targetTableMeta = validatorTargetTableAndGet(tables, insert.getTargetTable());
            Set<TableMetadata> sources = new HashSet<>();
            extractTables(insert.getSource(), tables,  sources);
            SqlParseHandler handler = HANDLERS.get(targetTableMeta.getType());
            if (handler == null) {
                throw new RuntimeException("cant find 'SqlParseHandler' for '" + targetTableMeta.getType() + "'");
            }
            lineages.addAll(handler.createTableLineages(sources, targetTableMeta));
        }
        return lineages;
    }

    private static void extractTables(SqlNode sqlNode, Map<String, TableMetadata> tables, Set<TableMetadata> result) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            // TODO: support 'EXCEPT', 'INTERSECT'
            case UNION:
                SqlBasicCall union = (SqlBasicCall) sqlNode;
                for (SqlNode operand : union.operands) {
                    extractTables(operand, tables, result);
                }
                break;

            case SELECT:
                SqlSelect select = (SqlSelect) sqlNode;
                extractTables(select.getFrom(), tables, result);
                break;

            case JOIN:
                SqlJoin join = (SqlJoin) sqlNode;
                extractTables(join.getLeft(), tables, result);
                extractTables(join.getRight(), tables, result);
                break;

            case AS:
                SqlBasicCall as = (SqlBasicCall) sqlNode;
                extractTables(as.getOperands()[0], tables, result);
                break;

            case IDENTIFIER:
                SqlIdentifier identifier = (SqlIdentifier) sqlNode;
                String table = join(identifier.names, ".");
                TableMetadata metadata = tables.get(table);
                if (metadata == null) {
                    throw new RuntimeException("cant find table definition, name: " + table);
                }
                result.add(metadata);
                break;

            case LATERAL:  // Lateral Table 维表血缘暂不支持
                break;

            case SNAPSHOT: // Temporal Table 维表血缘支持
                SqlSnapshot snapshot = (SqlSnapshot) sqlNode;
                extractTables(snapshot.getTableRef(), tables, result);
                break;

            default:
                break;
        }
    }

    private static TableMetadata validatorTargetTableAndGet(Map<String, TableMetadata> tableInfos,
            SqlNode targetTable) {
        if (targetTable.getKind() != SqlKind.IDENTIFIER) {
            throw new RuntimeException("target table should be 'IDENTIFIER'.");
        }
        String targetTableName = join(((SqlIdentifier) targetTable).names, ".");
        TableMetadata tableMeta = tableInfos.get(targetTableName);
        if (tableMeta == null) {
            throw new RuntimeException("cant find target table definition, name: " + targetTableName);
        }
        return tableMeta;
    }

    private static SqlParser.Config getSqlParserConfig() {
        return  SqlParser.config()
                .withParserFactory(FlinkSqlParserFactories.create(DEFAULT_SQL_CONFORMANCE))
                .withConformance(DEFAULT_SQL_CONFORMANCE)
                .withLex(Lex.JAVA)
                .withIdentifierMaxLength(256);
    }

    private static String combineSql(WarehouseJobTask task) {
        StringBuilder sb = new StringBuilder();
        sb.append(join(task.getMaterials(), ";\n"));
        sb.append(";\n");
        sb.append(join(task.getCalculates(), ";\n"));
        return sb.toString();
    }

}
