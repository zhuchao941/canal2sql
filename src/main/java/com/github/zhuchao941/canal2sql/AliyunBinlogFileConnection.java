package com.github.zhuchao941.canal2sql;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.aliyun.rds20140815.models.DescribeBinlogFilesRequest;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponse;
import com.aliyun.rds20140815.models.DescribeBinlogFilesResponseBody;
import com.taobao.tddl.dbsync.binlog.LogContext;
import com.taobao.tddl.dbsync.binlog.LogDecoder;
import com.taobao.tddl.dbsync.binlog.LogEvent;
import com.taobao.tddl.dbsync.binlog.LogPosition;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class AliyunBinlogFileConnection implements ErosaConnection {

    private static final Logger logger = LoggerFactory.getLogger(AliyunBinlogFileConnection.class);
    private Date startTime;
    private Date endTime;
    private String instanceId;
    private String ak;
    private String sk;
    private int bufferSize = 16 * 1024;
    private boolean running = false;
    private LogEventFilter logEventFilter;

    public AliyunBinlogFileConnection() {
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
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        String[] split = instanceId.split("\\|");
        DescribeBinlogFilesResponse describeBinlogFilesResponse = AliyunClient.describeBinlogFiles(new DescribeBinlogFilesRequest().setDBInstanceId(split[0]).setStartTime(format.format(startTime)).setEndTime(format.format(endTime)), ak, sk);
        List<DescribeBinlogFilesResponseBody.DescribeBinlogFilesResponseBodyItemsBinLogFile> binLogFiles = describeBinlogFilesResponse.getBody().getItems().getBinLogFile();
        if (split.length > 1) {
            binLogFiles = binLogFiles.stream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(split[1])).collect(Collectors.toList());
        }
        System.out.println("find binLogFile num:" + binLogFiles.size());
        binLogFiles.sort(Comparator.comparing(o -> o.logBeginTime));
        for (DescribeBinlogFilesResponseBody.DescribeBinlogFilesResponseBodyItemsBinLogFile binlogFile : binLogFiles) {
            System.out.println(String.format("fileName:%s, fileSize:%s, time:%s-%s", binlogFile.getLogFileName(), binlogFile.getFileSize(), binlogFile.getLogBeginTime(), binlogFile.getLogEndTime()));
        }

        for (DescribeBinlogFilesResponseBody.DescribeBinlogFilesResponseBodyItemsBinLogFile logFile : binLogFiles) {
            try (BinlogFileLogFetcher fetcher = new BinlogFileLogFetcher(bufferSize)) {
                LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
                LogContext context = new LogContext();
                fetcher.open(logFile.downloadLink, binlogPosition);
                context.setLogPosition(new LogPosition(binlogfilename, binlogPosition));
                LogEvent event = null;
                while (fetcher.fetch()) {
                    event = decoder.decode(fetcher, context);
                    if (logEventFilter != null) {
                        event = logEventFilter.filter(event);
                    }
                    if (event == null) {
                        continue;
                    }

                    if (!coprocessor.publish(event)) {
                        break;
                    }
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
        AliyunBinlogFileConnection connection = new AliyunBinlogFileConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setLogEventFilter(this.logEventFilter);

        return connection;
    }

    @Override
    public long queryServerId() {
        return 0;
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

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setAk(String ak) {
        this.ak = ak;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }
}
