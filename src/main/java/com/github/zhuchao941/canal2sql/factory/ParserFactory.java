package com.github.zhuchao941.canal2sql.factory;

import com.alibaba.otter.canal.parse.inbound.mysql.AbstractMysqlEventParser;
import com.github.zhuchao941.canal2sql.starter.Configuration;

public class ParserFactory {

    public static AbstractMysqlEventParser createParser(Configuration configuration) {
        String mode = configuration.getMode();
        if ("online".equals(mode)) {
            return new OnlineParserBuilder(configuration).build();
        } else if ("file".equals(mode)) {
            return new FileParserBuilder(configuration).build();
        } else if ("aliyun".equals(mode)) {
            return new AliyunParserBuilder(configuration).build();
        }
        throw new IllegalArgumentException("unsupported mode");
    }
}
