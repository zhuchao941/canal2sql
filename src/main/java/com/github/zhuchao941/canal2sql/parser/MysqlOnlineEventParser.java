package com.github.zhuchao941.canal2sql.parser;

import com.alibaba.otter.canal.parse.inbound.ErosaConnection;
import com.alibaba.otter.canal.parse.inbound.MultiStageCoprocessor;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlMultiStageCoprocessor;
import com.github.zhuchao941.canal2sql.LogEventFilter;

public class MysqlOnlineEventParser extends MysqlEventParser {

    private LogEventFilter logEventFilter;

    @Override
    protected MultiStageCoprocessor buildMultiStageCoprocessor() {
        MultiStageCoprocessor multiStageCoprocessor = super.buildMultiStageCoprocessor();
        ((MysqlMultiStageCoprocessor)multiStageCoprocessor).setLogEventFilter(this.logEventFilter);
        return multiStageCoprocessor;
    }

    public void setLogEventFilter(LogEventFilter logEventFilter) {
        this.logEventFilter = logEventFilter;
    }

    @Override
    protected ErosaConnection buildErosaConnection() {
        return super.buildErosaConnection();
    }
}
