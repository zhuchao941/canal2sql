package com.github.zhuchao941.canal2sql.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Canal2SqlUtils {

    public static String binlog2Insert(CanalEntry.Entry entry, List<CanalEntry.Column> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append("`").append(entry.getHeader().getSchemaName()).append("`.`").append(entry.getHeader().getTableName()).append("`(");
        sb.append(columns.stream().map(column -> "`" + column.getName() + "`").collect(Collectors.joining(",")));
        sb.append(") VALUES(");
        sb.append(columns.stream().map(column -> getValue(column)).collect(Collectors.joining(",")));
        sb.append(");");
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
            System.out.println("#" + logfileOffset + " " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(entry.getHeader().getExecuteTime())));
        }
        System.out.println(sql);
    }

    private static String getValue(CanalEntry.Column column) {
        if (column.getIsNull()) {
            return "NULL";
        }
        String mysqlType = column.getMysqlType();
        if (mysqlType.contains("varchar") || mysqlType.contains("text") || mysqlType.contains("json")) {
            return "'" + column.getValue() + "'";
        }
        return column.getValue();
    }
}
