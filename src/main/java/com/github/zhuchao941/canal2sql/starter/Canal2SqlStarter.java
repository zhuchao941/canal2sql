package com.github.zhuchao941.canal2sql.starter;

import com.github.zhuchao941.canal2sql.Canal2Sql;
import org.apache.commons.cli.*;
import org.springframework.util.StringUtils;

import java.text.SimpleDateFormat;

public class Canal2SqlStarter {

    public static void main(String[] args) {

        Long start = System.currentTimeMillis();

        Options options = new Options();

        Option rollback = new Option("B", "rollback", false, "Rollback parameter, default is false");
        options.addOption(rollback);

        Option append = new Option("A", "append", false, "Append parameter, default is false");
        options.addOption(append);

        Option user = new Option("u", "username", true, "Username");
        options.addOption(user);

        Option password = new Option("p", "password", true, "Password");
        options.addOption(password);

        Option port = new Option("P", "port", true, "Port");
        options.addOption(port);

        Option host = new Option("h", "host", true, "Database host");
        options.addOption(host);

        Option dir = new Option("dir", true, "Specify local binlog dir");
        options.addOption(dir);

        Option fileUrl = new Option("file_url", true, "Specify binlog file url");
        options.addOption(fileUrl);

        Option ddl = new Option("ddl", true, "Specify local ddl");
        options.addOption(ddl);

        Option startDatetime = new Option("start_time", true, "Start datetime");
        options.addOption(startDatetime);

        Option endDatetime = new Option("end_time", true, "End datetime");
        options.addOption(endDatetime);

        Option startPosition = new Option("start_position", true, "Start position");
        options.addOption(startPosition);

        Option endPosition = new Option("end_position", true, "End position");
        options.addOption(endPosition);

        Option filter = new Option("filter", true, "Specify filter configuration");
        options.addOption(filter);

        Option blackFilter = new Option("black_filter", true, "Specify blacklist configuration");
        options.addOption(blackFilter);

        Option mode = new Option("mode", true, "Specify running mode");
        mode.setRequired(true);
        options.addOption(mode);

        Option instanceId = new Option("instanceId", true, "Specify instanceId");
        options.addOption(instanceId);

        Option ak = new Option("ak", true, "Specify ak");
        options.addOption(ak);

        Option sk = new Option("sk", true, "Specify sk");
        options.addOption(sk);

        Option external = new Option("E", "external", false, "External parameter, default is false");
        options.addOption(external);

        Option minimal = new Option("M", "minimal", false, "Minimal parameter, default is false");
        options.addOption(minimal);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java -jar canal2sql-1.0.0-SNAPSHOT.jar", options);
            System.exit(1);
            return;
        }

        // Get all the input values
        boolean rollbackInput = cmd.hasOption("rollback");
        boolean appendInput = cmd.hasOption("append");
        String userInput = cmd.getOptionValue("username");
        String passwordInput = cmd.getOptionValue("password");
        String portInput = cmd.getOptionValue("port");
        String hostInput = cmd.getOptionValue("host");
        String fileUrlInput = cmd.getOptionValue("file_url");
        String dirInput = cmd.getOptionValue("dir");
        String ddlInput = cmd.getOptionValue("ddl");
        String startDatetimeInput = cmd.getOptionValue("start_time");
        String endDatetimeInput = cmd.getOptionValue("end_time");
        String startPositionInput = cmd.getOptionValue("start_position");
        String endPositionInput = cmd.getOptionValue("end_position");
        String filterInput = cmd.getOptionValue("filter");
        String blackFilterInput = cmd.getOptionValue("black_filter");
        String modeInput = cmd.getOptionValue("mode");
        String instanceIdInput = cmd.getOptionValue("instanceId");
        String akInput = cmd.getOptionValue("ak");
        String skInput = cmd.getOptionValue("sk");
        boolean externalInput = cmd.hasOption("external");
        String sqlTypeInput = cmd.getOptionValue("sql_type");
        boolean minimalInput = cmd.hasOption("minimal");

        // Print the input values
        System.out.println("# Mode: " + modeInput);
        System.out.println("# Rollback: " + rollbackInput);
        System.out.println("# Append: " + appendInput);
        System.out.println("# Username: " + userInput);
        System.out.println("# Password: " + passwordInput);
        System.out.println("# Port: " + portInput);
        System.out.println("# Host: " + hostInput);
        System.out.println("# File Url: " + fileUrlInput);
        System.out.println("# DIR: " + dirInput);
        System.out.println("# DDL: " + ddlInput);
        System.out.println("# Start datetime: " + startDatetimeInput);
        System.out.println("# End datetime: " + endDatetimeInput);
        System.out.println("# Start position: " + startPositionInput);
        System.out.println("# End position: " + endPositionInput);
        System.out.println("# Filter: " + filterInput);
        System.out.println("# Blacklist filter: " + blackFilterInput);
        System.out.println("# InstanceId: " + instanceIdInput);
        System.out.println("# Ak: " + akInput);
        System.out.println("# Sk: " + skInput);
        System.out.println("# External: " + externalInput);
        System.out.println("# SqlType: " + sqlTypeInput);
        System.out.println("# Minimal: " + minimalInput);
        System.out.println();

        // Do something with the input values

        Configuration config = new Configuration();

        // Set the values in the Configuration object
        config.setRollback(rollbackInput);
        config.setAppend(appendInput);
        config.setUsername(userInput);
        config.setPassword(passwordInput);
        config.setPort(StringUtils.isEmpty(portInput) ? null : Integer.parseInt(portInput));
        config.setHost(hostInput);
        config.setDdl(ddlInput);
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            config.setStartDatetime(StringUtils.isEmpty(startDatetimeInput) ? null : simpleDateFormat.parse(startDatetimeInput));
            config.setEndDatetime(StringUtils.isEmpty(endDatetimeInput) ? null : simpleDateFormat.parse(endDatetimeInput));
        } catch (java.text.ParseException e) {
            throw new RuntimeException(e);
        }
        config.setStartPosition(startPositionInput);
        config.setEndPosition(endPositionInput);
        config.setFilter(filterInput);
        config.setBlackFilter(blackFilterInput);
        config.setDir(dirInput);
        config.setFileUrl(fileUrlInput);
        config.setMode(modeInput);
        config.setInstanceId(instanceIdInput);
        config.setAk(akInput);
        config.setSk(skInput);
        config.setInternal(!externalInput);
        config.setSqlType(sqlTypeInput);
        config.setMinimal(minimalInput);

        new Canal2Sql().run(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> System.out.println("# totally cost:" + (System.currentTimeMillis() - start) + " ms")));
    }
}
