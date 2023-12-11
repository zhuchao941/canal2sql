package com.github.zhuchao941.canal2sql.factory;

import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.github.zhuchao941.canal2sql.filter.LogEventFilter;
import com.github.zhuchao941.canal2sql.parser.AliyunBinlogFileEventParser;
import com.github.zhuchao941.canal2sql.parser.MysqlOnlineEventParser;
import com.github.zhuchao941.canal2sql.starter.Configuration;
import org.apache.commons.lang.StringUtils;

import java.net.InetSocketAddress;

public class AliyunParserBuilder {

    private Configuration configuration;

    public AliyunParserBuilder(Configuration configuration) {
        this.configuration = configuration;
    }

    public AliyunBinlogFileEventParser build() {
        AliyunBinlogFileEventParser parser = new AliyunBinlogFileEventParser();
        if (org.apache.commons.lang.StringUtils.isNotBlank(configuration.getHost())) {
            parser.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(configuration.getHost(), configuration.getPort()), configuration.getUsername(), configuration.getPassword()));
        }
        parser.setDdlFile(configuration.getDdl());
        // 这里后续dump不依赖journalName了
        EntryPosition entryPosition = new EntryPosition("rdsFile", 0L);
        parser.setMasterPosition(entryPosition);
        Long startPosition = StringUtils.isBlank(configuration.getStartPosition()) ? null : Long.parseLong(configuration.getStartPosition());
        Long endPosition = StringUtils.isBlank(configuration.getEndPosition()) ? null : Long.parseLong(configuration.getEndPosition());
        parser.setLogEventFilter(new LogEventFilter(configuration.getStartDatetime(), configuration.getEndDatetime(), startPosition, endPosition));
        parser.setStartTime(configuration.getStartDatetime());
        parser.setEndTime(configuration.getEndDatetime());
        parser.setInstanceId(configuration.getInstanceId());
        parser.setAk(configuration.getAk());
        parser.setSk(configuration.getSk());
        parser.setInternal(configuration.isInternal());
        return parser;
    }

    class FileWithPosition {
        private String fileName;
        private Long position;

        public FileWithPosition(String fileName, Long position) {
            this.fileName = fileName;
            this.position = position;
        }

        public String getFileName() {
            return fileName;
        }

        public Long getPosition() {
            return position;
        }
    }

    private FileWithPosition extract(String fileWithPositionStr) {
        String[] split = fileWithPositionStr.split("\\|");
        if (split.length != 2) {
            throw new IllegalArgumentException("must fileName|position");
        } else {
            return new FileWithPosition(split[0], Long.parseLong(split[1]));
        }
    }
}
