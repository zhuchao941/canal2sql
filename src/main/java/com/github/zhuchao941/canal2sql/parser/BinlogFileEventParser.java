package com.github.zhuchao941.canal2sql.parser;

import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert;
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.github.zhuchao941.canal2sql.LogEventFilter;
import com.github.zhuchao941.canal2sql.BinlogFileConnection;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 基于本地binlog文件的复制
 *
 * @author jianghang 2012-6-21 下午04:07:33
 * @version 1.0.0
 */
public class BinlogFileEventParser extends AbstractMysqlEventParser implements CanalEventParser {

    // 数据库信息
    protected AuthenticationInfo masterInfo = new AuthenticationInfo(new InetSocketAddress("localhost", 3306), "", "");
    protected EntryPosition masterPosition;        // binlog信息
    protected MysqlConnection metaConnection;        // 查询meta信息的链接
    protected TableMetaCache tableMetaCache;        // 对应meta
    protected int bufferSize = 16 * 1024;

    protected String ddlFile;
    protected String binlogFile;

    protected LogEventFilter logEventFilter;

    public BinlogFileEventParser() {
        // this.runningInfo = new AuthenticationInfo();
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        return buildLocalBinLogConnection();
    }

    @Override
    protected void preDump(ErosaConnection connection) {
        metaConnection = buildMysqlConnection();
        try {
            metaConnection.connect();
        } catch (IOException e) {
            throw new CanalParseException(e);
        }
        if (tableMetaTSDB != null && tableMetaTSDB instanceof DatabaseTableMeta) {
            ((DatabaseTableMeta) tableMetaTSDB).setFilter(eventFilter);
            ((DatabaseTableMeta) tableMetaTSDB).setBlackFilter(eventBlackFilter);
            ((DatabaseTableMeta) tableMetaTSDB).setSnapshotInterval(tsdbSnapshotInterval);
            ((DatabaseTableMeta) tableMetaTSDB).setSnapshotExpire(tsdbSnapshotExpire);
            ((DatabaseTableMeta) tableMetaTSDB).init(destination);
        }
        if (StringUtils.isNotBlank(ddlFile)) {
        tableMetaCache = new TableMetaCache(ddlFile);
        } else {
            tableMetaCache = new TableMetaCache(metaConnection, tableMetaTSDB);
        }
        ((LogEventConvert) binlogParser).setTableMetaCache(tableMetaCache);
    }

    @Override
    protected void afterDump(ErosaConnection connection) {
        try {
            // 延迟一会 让DIsruptor可以正常消费完
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.exit(1);
    }

    public void start() throws CanalParseException {
        if (runningInfo == null) { // 第一次链接主库
            runningInfo = masterInfo;
        }

        super.start();
    }

    @Override
    public void stop() {
        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }

        super.stop();
    }

    private ErosaConnection buildLocalBinLogConnection() {
        BinlogFileConnection connection = new BinlogFileConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setBinlogFile(this.binlogFile);
        connection.setLogEventFilter(this.logEventFilter);

        return connection;
    }

    private MysqlConnection buildMysqlConnection() {
        MysqlConnection connection = new MysqlConnection(runningInfo.getAddress(),
                runningInfo.getUsername(),
                runningInfo.getPassword(),
                connectionCharsetNumber,
                runningInfo.getDefaultDatabaseName());
        connection.getConnector().setReceiveBufferSize(64 * 1024);
        connection.getConnector().setSendBufferSize(64 * 1024);
        connection.getConnector().setSoTimeout(30 * 1000);
        connection.setCharset(connectionCharset);
        return connection;
    }
    @Override
    protected EntryPosition findStartPosition(ErosaConnection connection) {
        // 处理逻辑
        // 1. 首先查询上一次解析成功的最后一条记录
        // 2. 存在最后一条记录，判断一下当前记录是否发生过主备切换
        // // a. 无机器切换，直接返回
        // // b. 存在机器切换，按最后一条记录的stamptime进行查找
        // 3. 不存在最后一条记录，则从默认的位置开始启动
        LogPosition logPosition = logPositionManager.getLatestIndexBy(destination);
        if (logPosition == null) {// 找不到历史成功记录
            EntryPosition entryPosition = masterPosition;

            // 判断一下是否需要按时间订阅
            if (StringUtils.isEmpty(entryPosition.getJournalName())) {
                // 如果没有指定binlogName，尝试按照timestamp进行查找
                if (entryPosition.getTimestamp() != null) {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            } else {
                if (entryPosition.getPosition() != null) {
                    // 如果指定binlogName + offest，直接返回
                    return entryPosition;
                } else {
                    return new EntryPosition(entryPosition.getTimestamp());
                }
            }
        } else {
            return logPosition.getPostion();
        }

        return null;
    }

    // ========================= setter / getter =========================

    public void setLogPositionManager(CanalLogPositionManager logPositionManager) {
        this.logPositionManager = logPositionManager;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setMasterPosition(EntryPosition masterPosition) {
        this.masterPosition = masterPosition;
    }

    public void setMasterInfo(AuthenticationInfo masterInfo) {
        this.masterInfo = masterInfo;
    }

    public void setLogEventFilter(LogEventFilter logEventFilter) {
        this.logEventFilter = logEventFilter;
    }

    public void setDdlFile(String ddlFile) {
        this.ddlFile = ddlFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }
}
