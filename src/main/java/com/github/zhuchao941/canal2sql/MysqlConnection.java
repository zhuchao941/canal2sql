package com.github.zhuchao941.canal2sql;

import com.alibaba.otter.canal.parse.driver.mysql.packets.GTIDSet;
import com.alibaba.otter.canal.parse.inbound.SinkFunction;
import com.taobao.tddl.dbsync.binlog.LogEvent;

import java.io.IOException;

public class MysqlConnection extends com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection {

    private LogEventFilter logEventFilter;

    public void setLogEventFilter(LogEventFilter logEventFilter) {
        this.logEventFilter = logEventFilter;
    }

    @Override
    public void dump(GTIDSet gtidSet, SinkFunction func) throws IOException {
        super.dump(gtidSet, o -> {
            if (o instanceof LogEvent) {
                LogEvent logEvent = logEventFilter.filter((LogEvent) o);
                if (logEvent == null) {
                    return false;
                }
            }
            return func.sink(o);
        });
    }

    @Override
    public void dump(String binlogfilename, Long binlogPosition, SinkFunction func) throws IOException {
        super.dump(binlogfilename, binlogPosition, o -> {
            if (o instanceof LogEvent) {
                LogEvent logEvent = logEventFilter.filter((LogEvent) o);
                if (logEvent == null) {
                    return false;
                }
            }
            return func.sink(o);
        });
    }
}
