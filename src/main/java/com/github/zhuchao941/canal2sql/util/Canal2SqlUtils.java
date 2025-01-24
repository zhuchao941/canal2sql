package com.github.zhuchao941.canal2sql.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Canal2SqlUtils {

    private static final List<String> typesRequiringQuotes = Arrays.asList("char", "varchar", "binary", "varbinary", "longtext", "text", "enum", "set", "json", "date", "datetime", "timestamp", "time", "year");
    // 预编译正则表达式模式
    private static final Pattern PATTERN_SINGLE_QUOTE = Pattern.compile("'");

    public static String binlog2Insert(CanalEntry.Entry entry, List<CanalEntry.Column> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("`(");
        sb.append(columns.stream().map(column -> "`" + column.getName() + "`").collect(Collectors.joining(",")));
        sb.append(") VALUES(");
        sb.append(columns.stream().map(column -> getValue(column)).collect(Collectors.joining(",")));
        sb.append(");");
        return sb.toString();
    }

    public static String binlog2BatchInsert(CanalEntry.Entry entry, boolean rollback, List<CanalEntry.RowData> rowDataList) {
        List<CanalEntry.Column> columns = !rollback ? rowDataList.get(0).getAfterColumnsList() : rowDataList.get(0).getBeforeColumnsList();
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("`(");
        sb.append(columns.stream().map(column -> "`" + column.getName() + "`").collect(Collectors.joining(",")));
        sb.append(") VALUES");
        for (int i = 0; i < rowDataList.size(); i++) {
            sb.append("(");
            CanalEntry.RowData rowData = rowDataList.get(i);
            List<CanalEntry.Column> demoList = !rollback ? rowData.getAfterColumnsList() : rowData.getBeforeColumnsList();
            sb.append(demoList.stream().map(column -> getValue(column)).collect(Collectors.joining(",")));
            sb.append(")");
            if (i < rowDataList.size() - 1) {
                sb.append(", ");
            } else {
                sb.append(";");
            }
        }
        return sb.toString();
    }

    public static String binlog2BatchDelete(CanalEntry.Entry entry, boolean rollback, List<CanalEntry.RowData> rowDataList) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("` WHERE ");
        for (int i = 0; i < rowDataList.size(); i++) {
            List<CanalEntry.Column> demoList = !rollback ? rowDataList.get(i).getBeforeColumnsList() : rowDataList.get(i).getAfterColumnsList();
            List<CanalEntry.Column> pkList = demoList.stream().filter(column -> column.getIsKey()).collect(Collectors.toList());
            for (int j = 0; j < pkList.size(); j++) {
                CanalEntry.Column column = pkList.get(j);
                sb.append("(`").append(column.getName()).append("` = ").append(getValue(column));
                if (j < pkList.size() - 1) {
                    sb.append(" and ");
                } else {
                    sb.append(")");
                }
            }
            if (i < rowDataList.size() - 1) {
                sb.append(" or ");
            } else {
                sb.append(";");
            }
        }
        return sb.toString();
    }

    public static String binlog2Delete(CanalEntry.Entry entry, List<CanalEntry.Column> pkList) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("` WHERE ");
        for (int i = 0; i < pkList.size(); i++) {
            CanalEntry.Column column = pkList.get(i);
            sb.append("`").append(column.getName()).append("` = ").append(getValue(column));
            if (i < pkList.size() - 1) {
                sb.append(" and ");
            } else {
                sb.append(";");
            }
        }
        return sb.toString();
    }

    public static String binlog2Update(CanalEntry.Entry entry, List<CanalEntry.Column> updatedColumnsList, List<CanalEntry.Column> pkList) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("` SET ");
        sb.append(updatedColumnsList.stream().map(column -> "`" + column.getName() + "` = " + getValue(column)).collect(Collectors.joining(",")));
        sb.append(" WHERE ");
        for (int i = 0; i < pkList.size(); i++) {
            CanalEntry.Column column = pkList.get(i);
            sb.append("`").append(column.getName()).append("` = ").append(getValue(column));
            if (i < pkList.size() - 1) {
                sb.append(" and ");
            } else {
                sb.append(";");
            }
        }
        return sb.toString();
    }

    public static void printSql(boolean rollback, boolean append, AtomicBoolean logged, long logfileOffset, CanalEntry.Entry entry, Function<Object, String> sqlFunction, Function<Object, String> rollbackFunction) {
        String sql = "";
        String rollbackSql = "";
        if (append || rollback) {
            rollbackSql = rollbackFunction.apply(null);
        }
        if (append || !rollback) {
            sql = sqlFunction.apply(null);
        }
        if (rollback && append) {
            sql = rollbackSql + " # " + sql;
        } else if (!rollback && append) {
            sql = sql + " # " + rollbackSql;
        } else if (rollback) {
            sql = rollbackSql;
        }
        if (logged.compareAndSet(false, true)) {
            String logfileName = entry.getHeader().getLogfileName();
            System.out.println("#" + logfileName + ":" + logfileOffset + " " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(entry.getHeader().getExecuteTime())));
        }
        System.out.println(sql);
    }

    private static String getValue(CanalEntry.Column column) {
        if (column.getIsNull()) {
            return "NULL";
        }
        String mysqlType = column.getMysqlType();
        String[] split = mysqlType.split("\\(");
        if (split.length != 1) {
            mysqlType = split[0];
        }
        if (typesRequiringQuotes.contains(mysqlType)) {
            // 使用预编译的模式进行替换
            Matcher matcher = PATTERN_SINGLE_QUOTE.matcher(column.getValue());
            String escapedStr = matcher.replaceAll("\\\\'");
            return "'" + escapedStr + "'";
        }else if ("longblob".equals(mysqlType) || "blob".equals(mysqlType)) {
            String result = new String(column.getValue().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
            return "'" + result + "'";
        }
        return column.getValue();
    }
}
