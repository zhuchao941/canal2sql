package com.github.zhuchao941.canal2sql.filter;

import com.alibaba.otter.canal.parse.exception.ServerIdNotMatchException;
import com.taobao.tddl.dbsync.binlog.LogEvent;

import java.util.Date;

public class LogEventFilter {

    private long serverId;
    private Date startTime;
    private Date endTime;
    private Long startPosition;
    private Long endPosition;

    private String startFile;
    private String endFile;

    public LogEventFilter(Date startTime, Date endTime, Long startPosition, Long endPosition) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
    }

    public LogEventFilter(Date startTime, Date endTime, Long startPosition, Long endPosition, String startFile, String endFile) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.startPosition = startPosition;
        this.endPosition = endPosition;
        this.startFile = startFile;
        this.endFile = endFile;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(Long startPosition) {
        this.startPosition = startPosition;
    }

    public Long getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(Long endPosition) {
        this.endPosition = endPosition;
    }

    public LogEvent filter(LogEvent event) {
        if (event == null) {
            return null;
        }
        String logFileName = event.getHeader().getLogFileName();
        if (startTime != null && event.getWhen() < startTime.getTime() / 1000) {
            return null;
        }
        if (endTime != null && event.getWhen() > endTime.getTime() / 1000) {
            shutdownLater();
            return null;
        }
        // binlog 文件需要从头遍历，而online模式可以直接从指定位置读
        if (startFile == null) {
            if (startPosition != null && event.getLogPos() < startPosition) {
                return null;
            }
        } else {
            if (startFile.equals(logFileName) && startPosition != null && event.getLogPos() < startPosition) {
                return null;
            }
        }

        if (endFile == null) {
            if (endPosition != null && event.getLogPos() > endPosition) {
                shutdownLater();
                return null;
            }
        } else {
            if (endFile.equals(logFileName) && endPosition != null && event.getLogPos() > endPosition) {
                shutdownLater();
                return null;
            }
        }

        if (serverId != 0 && event.getServerId() != serverId) {
            throw new ServerIdNotMatchException("unexpected serverId " + serverId + " in binlog file !");
        }
        return event;
    }

    private void shutdownLater() {
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.exit(1);
        }).start();
    }
}
