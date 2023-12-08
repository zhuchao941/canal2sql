package com.github.zhuchao941.canal2sql.parser;

import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.github.zhuchao941.canal2sql.AliyunBinlogFileConnection;

import java.util.Date;

/**
 * 基于本地binlog文件的复制
 *
 * @author jianghang 2012-6-21 下午04:07:33
 * @version 1.0.0
 */
public class AliyunBinlogFileEventParser extends BinlogFileEventParser {

    private Date startTime;
    private Date endTime;
    private String instanceId;
    private String ak;
    private String sk;
    private boolean internal;

    @Override
    protected ErosaConnection buildErosaConnection() {
        AliyunBinlogFileConnection connection = new AliyunBinlogFileConnection();

        connection.setBufferSize(this.bufferSize);
        connection.setStartTime(this.startTime);
        connection.setEndTime(this.endTime);
        connection.setInstanceId(this.instanceId);
        connection.setAk(this.ak);
        connection.setSk(this.sk);
        connection.setInternal(this.internal);
        connection.setLogEventFilter(this.logEventFilter);

        return connection;
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

    public void setInternal(boolean internal) {
        this.internal = internal;
    }
}
