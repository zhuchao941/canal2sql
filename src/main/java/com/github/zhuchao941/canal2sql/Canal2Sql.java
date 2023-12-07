package com.github.zhuchao941.canal2sql;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.github.zhuchao941.canal2sql.parser.LocalBinlogEventWithLocalDDLParser;
import com.github.zhuchao941.canal2sql.parser.MysqlOnlineEventParser;
import com.github.zhuchao941.canal2sql.starter.Configuration;
import com.github.zhuchao941.canal2sql.util.Canal2SqlUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Canal2Sql {

    public void run(Configuration configuration) {
        boolean rollback = configuration.isRollback();
        boolean append = configuration.isAppend();
        String directory = configuration.getDir();
        String binlogName = configuration.getBinlogName();
        Date startDatetime = configuration.getStartDatetime();
        Date endDatetime = configuration.getEndDatetime();
        Long startPosition = configuration.getStartPosition();
        AbstractMysqlEventParser parser;
        if (configuration.isOnline()) {
            parser = new MysqlOnlineEventParser();
            ((MysqlEventParser) parser).setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configuration.getHost(), configuration.getPort()), configuration.getUsername(), configuration.getPassword()));
            ((MysqlEventParser) parser).setMasterPosition(new EntryPosition(binlogName, 0L));
            ((MysqlOnlineEventParser) parser).setLogEventFilter(new LogEventFilter(startDatetime, endDatetime, startPosition, configuration.getEndPosition()));
        } else {
            parser = new LocalBinlogEventWithLocalDDLParser();
            Assert.notNull(configuration.getDdl(), "offline mode DDL cannot be null");
            Assert.notNull(binlogName, "offline mode Binlog name cannot be null");
            ((LocalBinlogEventWithLocalDDLParser) parser).setDdlFile(configuration.getDdl());
            EntryPosition entryPosition = new EntryPosition(binlogName, 0L);
            ((LocalBinlogEventWithLocalDDLParser) parser).setMasterPosition(entryPosition);
            ((LocalBinlogEventWithLocalDDLParser) parser).setLogEventFilter(new LogEventFilter(startDatetime, endDatetime, startPosition, configuration.getEndPosition()));
            ((LocalBinlogEventWithLocalDDLParser) parser).setDirectory(directory);
        }
        if (!StringUtils.isEmpty(configuration.getFilter())) {
            parser.setEventFilter(new AviaterRegexFilter(configuration.getFilter()));
        }
        if (!StringUtils.isEmpty(configuration.getBlackFilter())) {
            parser.setEventBlackFilter(new AviaterRegexFilter(configuration.getBlackFilter()));
        }
        parser.setEventSink(new AbstractCanalEventSink<List<Entry>>() {

            public boolean sink(List<Entry> entrys, InetSocketAddress remoteAddress, String destination) throws CanalSinkException {

                long logfileOffset = 0;
                for (Entry entry : entrys) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                        logfileOffset = entry.getHeader().getLogfileOffset();
                        continue;
                    }
                    if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        System.out.println();
                        logfileOffset = 0;
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

                        for (RowData rowData : rowChage.getRowDatasList()) {
                            if (eventType == EventType.DELETE) {
                                Canal2SqlUtils.printSql(rollback, append, logfileOffset, entry, o -> {
                                    List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
                                    List<Column> pkList = beforeColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                    return Canal2SqlUtils.binlog2Delete(entry, pkList);
                                }, o -> Canal2SqlUtils.binlog2Insert(entry, rowData.getBeforeColumnsList()));
                            } else if (eventType == EventType.INSERT) {
                                List<Column> afterColumnsList = rowData.getAfterColumnsList();
                                Canal2SqlUtils.printSql(rollback, append, logfileOffset, entry, o -> Canal2SqlUtils.binlog2Insert(entry, afterColumnsList), o -> {
                                    List<Column> pkList = afterColumnsList.stream().filter(i -> i.getIsKey()).collect(Collectors.toList());
                                    return Canal2SqlUtils.binlog2Delete(entry, pkList);
                                });
                            } else {
                                Canal2SqlUtils.printSql(rollback, append, logfileOffset, entry, o -> {
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
                return true;
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
