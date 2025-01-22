package com.github.zhuchao941.canal2sql;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.github.zhuchao941.canal2sql.factory.ParserFactory;
import com.github.zhuchao941.canal2sql.starter.Configuration;
import com.github.zhuchao941.canal2sql.util.Canal2SqlUtils;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Canal2Sql {

    public void run(Configuration configuration) {
        boolean rollback = configuration.isRollback();
        boolean append = configuration.isAppend();

        AbstractMysqlEventParser parser = ParserFactory.createParser(configuration);
        if (!StringUtils.isEmpty(configuration.getFilter())) {
            parser.setEventFilter(new AviaterRegexFilter(configuration.getFilter()));
        }
        if (!StringUtils.isEmpty(configuration.getBlackFilter())) {
            parser.setEventBlackFilter(new AviaterRegexFilter(configuration.getBlackFilter()));
        }
        String sqlType = configuration.getSqlType();
        if (org.apache.commons.lang.StringUtils.isBlank(sqlType)) {
            sqlType = "insert,update,delete,ddl";
        }
        sqlType = sqlType.toLowerCase();
        Set<String> printableSet = Arrays.stream(sqlType.split(",")).collect(Collectors.toSet());
        final AtomicLong logfileOffset = new AtomicLong(0);
        final AtomicBoolean logged = new AtomicBoolean(false);
        boolean clean = configuration.isClean();
        parser.setEventSink(new AbstractCanalEventSink<List<Entry>>() {

            // 这里都是单线程进来的
            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination) throws CanalSinkException {
                for (Entry entry : entrys) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                        logfileOffset.set(entry.getHeader().getLogfileOffset());
                        continue;
                    }
                    if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        if (logged.getAndSet(false)) {
                            System.out.println();
                        }
                        logfileOffset.set(0);
                        continue;
                    }

                    if (entry.getEntryType() == EntryType.ROWDATA) {
                        RowChange rowChage = null;
                        try {
                            rowChage = RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
                        }

                        EventType eventType = rowChage.getEventType();

                        final RowChange fRowChange = rowChage;

                        if (rowChage.getIsDdl() && printableSet.contains("ddl")) {
                            Canal2SqlUtils.printSql(clean, rollback, append, new AtomicBoolean(false),
                                    entry.getHeader().getLogfileOffset(), entry,
                                    o -> fRowChange.getSql().endsWith(";") ? fRowChange.getSql()
                                            : fRowChange.getSql() + ";",
                                    o -> "temporarily not support rollback sql for ddl");
                            System.out.println();
                            continue;
                        }

                        boolean minimal = configuration.isMinimal();
                        if ((eventType == EventType.DELETE || eventType == EventType.INSERT) && minimal && rowChage.getRowDatasList().size() > 1) {
                            if (enableDelete(eventType, printableSet)) {
                                List<RowData> rowDatasList = fRowChange.getRowDatasList();
                                Canal2SqlUtils.printSql(clean, rollback, append, logged, logfileOffset.get(), entry, o -> Canal2SqlUtils.binlog2BatchDelete(entry, false, rowDatasList), o -> Canal2SqlUtils.binlog2BatchInsert(entry, true, rowDatasList));
                            } else if (enableInsert(eventType, printableSet)) {
                                List<RowData> rowDatasList = fRowChange.getRowDatasList();
                                Canal2SqlUtils.printSql(clean, rollback, append, logged, logfileOffset.get(), entry, o -> Canal2SqlUtils.binlog2BatchInsert(entry, false, rowDatasList), o -> Canal2SqlUtils.binlog2BatchDelete(entry, true, rowDatasList));
                            }
                        } else {
                            for (RowData rowData : rowChage.getRowDatasList()) {
                                if (enableDelete(eventType, printableSet)) {
                                    Canal2SqlUtils.printSql(clean, rollback, append, logged, logfileOffset.get(), entry, o -> {
                                        List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
                                        List<Column> pkList = beforeColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                        return Canal2SqlUtils.binlog2Delete(entry, pkList);
                                    }, o -> Canal2SqlUtils.binlog2Insert(entry, rowData.getBeforeColumnsList()));
                                } else if (enableInsert(eventType, printableSet)) {
                                    List<Column> afterColumnsList = rowData.getAfterColumnsList();
                                    Canal2SqlUtils.printSql(clean, rollback, append, logged, logfileOffset.get(), entry, o -> Canal2SqlUtils.binlog2Insert(entry, afterColumnsList), o -> {
                                        List<Column> pkList = afterColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                        return Canal2SqlUtils.binlog2Delete(entry, pkList);
                                    });
                                } else if (enableUpdate(eventType, printableSet)) {
                                    Canal2SqlUtils.printSql(clean, rollback, append, logged, logfileOffset.get(), entry, o -> {
                                        List<Column> afterColumnsList = rowData.getAfterColumnsList();
                                        List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
                                        List<Column> pkList = beforeColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                        return Canal2SqlUtils.binlog2Update(entry, afterColumnsList.stream().filter(column -> column.getUpdated()).collect(Collectors.toList()), pkList);
                                    }, o -> {
                                        List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
                                        List<Column> afterColumnsList = rowData.getAfterColumnsList();
                                        Map<String, Column> updatedMap = afterColumnsList.stream().filter(column -> column.getUpdated()).collect(Collectors.toMap(k -> k.getName(), v -> v));
                                        List<Column> pkList = beforeColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                        List<Column> updatedColumnsList = beforeColumnsList.stream().filter(column -> updatedMap.get(column.getName()) != null).collect(Collectors.toList());
                                        return Canal2SqlUtils.binlog2Update(entry, updatedColumnsList, pkList);
                                    });
                                }
                            }
                        }
                    }
                }
                return true;
            }

            private boolean enableDelete(EventType eventType, Set<String> printableSet) {
                return eventType == EventType.DELETE && printableSet.contains(eventType.name().toLowerCase());
            }

            private boolean enableInsert(EventType eventType, Set<String> printableSet) {
                return eventType == EventType.INSERT && printableSet.contains(eventType.name().toLowerCase());
            }

            private boolean enableUpdate(EventType eventType, Set<String> printableSet) {
                return eventType == EventType.UPDATE && printableSet.contains(eventType.name().toLowerCase());
            }
        });
        parser.setLogPositionManager(new AbstractLogPositionManager() {
            @Override
            public LogPosition getLatestIndexBy(String destination) {
                return null;
            }

            @Override
            public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
            }
        });
        parser.start();
    }
}
