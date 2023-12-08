package com.github.zhuchao941.canal2sql.connection;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.github.zhuchao941.canal2sql.fetcher.BinlogFileLogFetcher;
import com.github.zhuchao941.canal2sql.filter.LogEventFilter;
import com.taobao.tddl.dbsync.binlog.*;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class BinlogFileConnection implements ErosaConnection {

    private static final Logger logger = LoggerFactory.getLogger(BinlogFileConnection.class);
    private String binlogFile;
    private int bufferSize = 16 * 1024;
    private boolean running = false;
    private LogEventFilter logEventFilter;

    public BinlogFileConnection() {
    }

    @Override
    public void connect() throws IOException {
        this.running = true;
    }

    @Override
    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    @Override
    public void disconnect() throws IOException {
        this.running = false;
    }

    public void seek(String binlogfilename, Long binlogPosition, String gtid, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    public void dump(long timestampMills, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, MultiStageCoprocessor coprocessor) throws IOException {
        try (BinlogFileLogFetcher fetcher = new BinlogFileLogFetcher(bufferSize)) {
            LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
            LogContext context = new LogContext();
            fetcher.open(binlogFile, binlogPosition);
            context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
            LogEvent event = null;
            while (fetcher.fetch()) {
                event = decoder.decode(fetcher, context);
                if (event == null) {
                    continue;
                }

                if (!coprocessor.publish(event)) {
                    break;
                }
            }
        }
    }

    @Override
    public void dump(long timestampMills, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void dump(GTIDSet gtidSet, MultiStageCoprocessor coprocessor) throws IOException {
        throw new NotImplementedException();
    }

    public ErosaConnection fork() {
        BinlogFileConnection connection = new BinlogFileConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setBinlogFile(this.binlogFile);
        connection.setLogEventFilter(this.logEventFilter);

        return connection;
    }

    @Override
    public long queryServerId() {
        return 0;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setLogEventFilter(LogEventFilter logEventFilter) {
        this.logEventFilter = logEventFilter;
    }
}
