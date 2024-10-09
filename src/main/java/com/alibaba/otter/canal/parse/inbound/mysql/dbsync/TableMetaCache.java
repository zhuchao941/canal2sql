package com.alibaba.otter.canal.parse.inbound.mysql.dbsync;

import com.alibaba.otter.canal.parse.driver.mysql.packets.server.FieldPacket;
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.DatabaseTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.MemoryTableMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.tsdb.TableMetaTSDB;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理table meta解析和缓存
 *
 * @author jianghang 2013-1-17 下午10:15:16
 * @version 1.0.0
 */
public class TableMetaCache {

    public static final String              COLUMN_NAME    = "COLUMN_NAME";
    public static final String              COLUMN_TYPE    = "COLUMN_TYPE";
    public static final String              IS_NULLABLE    = "IS_NULLABLE";
    public static final String              COLUMN_KEY     = "COLUMN_KEY";
    public static final String              COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String              EXTRA          = "EXTRA";

    private static final Pattern PATTERN = Pattern.compile("`(.*)`.`(.*)`");
    private MysqlConnection                 connection;
    private boolean                         isOnRDS        = false;
    private boolean                         isOnTSDB       = false;

    private TableMetaTSDB                   tableMetaTSDB;
    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private LoadingCache<String, TableMeta> tableMetaDB;

    public TableMetaCache(String fileName){

        String createDDLs;
        try {
            createDDLs = FileUtils.readFileToString(new File(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        MemoryTableMeta memoryTableMeta = new MemoryTableMeta();

        String[] createDDLList = createDDLs.split(";");
        for (String createDDL : createDDLList) {
            memoryTableMeta.apply(DatabaseTableMeta.INIT_POSITION, null, createDDL, null);
        }
        this.tableMetaDB = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {

            @Override
            public TableMeta load(String name) throws Exception {
                String[] arr = extractSchemaAndTableName(name);
                if (arr == null) {
                    return null;
                }
                String schema = arr[0];
                String table = arr[1];
                return memoryTableMeta.find(schema, table);
            }
        });

    }

    public TableMetaCache(MysqlConnection con, TableMetaTSDB tableMetaTSDB){
        this.connection = con;
        this.tableMetaTSDB = tableMetaTSDB;
        // 如果持久存储的表结构为空，从db里面获取下
        if (tableMetaTSDB == null) {
            this.tableMetaDB = CacheBuilder.newBuilder().build(new CacheLoader<String, TableMeta>() {

                @Override
                public TableMeta load(String name) throws Exception {
                    try {
                        return getTableMetaByDB(name);
                    } catch (Throwable e) {
                        // 尝试做一次retry操作
                        try {
                            connection.reconnect();
                            return getTableMetaByDB(name);
                        } catch (IOException e1) {
                            throw new CanalParseException("fetch failed by table meta:" + name, e1);
                        }
                    }
                }

            });
        } else {
            isOnTSDB = true;
        }

        try {
            ResultSetPacket packet = connection.query("show global variables  like 'rds\\_%'");
            if (packet.getFieldValues().size() > 0) {
                isOnRDS = true;
            }
        } catch (IOException e) {
        }
    }

    private static String[] extractSchemaAndTableName(String fullname) {
        String[] names = StringUtils.split(fullname, "`.`");
        if (names.length == 2) {
            String schema = names[0];
            String table = names[1].substring(0, names[1].length());
            return new String[]{schema, table};
        } else if (names.length > 2) {
            Matcher matcher = PATTERN.matcher(fullname);
            if (matcher.find()) {
                String schema = matcher.group(1);
                String table = matcher.group(2);
                return new String[]{schema, table};
            }
        }
        return null;
    }

    private synchronized TableMeta getTableMetaByDB(String fullname) throws IOException {
        ResultSetPacket packet = null;
        try {
            packet = connection.query("show create table " + fullname);
        } catch (Throwable e) { // fallback to desc table
            packet = connection.query("desc " + fullname);
        }

        String[] arr = extractSchemaAndTableName(fullname);
        if (arr == null) {
            return null;
        }
        String schema = arr[0];
        String table = arr[1];
        return new TableMeta(schema, table, parseTableMeta(schema, table, packet));
    }

    public static List<FieldMeta> parseTableMeta(String schema, String table, ResultSetPacket packet) {
        if (packet.getFieldValues().size() > 1) {
            String createDDL = packet.getFieldValues().get(1);
            MemoryTableMeta memoryTableMeta = new MemoryTableMeta();
            memoryTableMeta.apply(DatabaseTableMeta.INIT_POSITION, schema, createDDL, null);
            TableMeta tableMeta = memoryTableMeta.find(schema, table);
            return tableMeta.getFields();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * 处理desc table的结果
     */
    public static List<FieldMeta> parseTableMetaByDesc(ResultSetPacket packet) {
        Map<String, Integer> nameMaps = new HashMap<>(6, 1f);
        int index = 0;
        for (FieldPacket fieldPacket : packet.getFieldDescriptors()) {
            nameMaps.put(fieldPacket.getOriginalName(), index++);
        }

        int size = packet.getFieldDescriptors().size();
        int count = packet.getFieldValues().size() / packet.getFieldDescriptors().size();
        List<FieldMeta> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            FieldMeta meta = new FieldMeta();
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
            meta.setColumnName(packet.getFieldValues().get(nameMaps.get(COLUMN_NAME) + i * size).intern());
            meta.setColumnType(packet.getFieldValues().get(nameMaps.get(COLUMN_TYPE) + i * size));
            meta.setNullable(StringUtils.equalsIgnoreCase(packet.getFieldValues().get(nameMaps.get(IS_NULLABLE) + i
                                                                                      * size),
                "YES"));
            meta.setKey("PRI".equalsIgnoreCase(packet.getFieldValues().get(nameMaps.get(COLUMN_KEY) + i * size)));
            meta.setUnique("UNI".equalsIgnoreCase(packet.getFieldValues().get(nameMaps.get(COLUMN_KEY) + i * size)));
            // 特殊处理引号
            meta.setDefaultValue(DruidDdlParser.unescapeQuotaName(packet.getFieldValues()
                .get(nameMaps.get(COLUMN_DEFAULT) + i * size)));
            meta.setExtra(packet.getFieldValues().get(nameMaps.get(EXTRA) + i * size));

            result.add(meta);
        }

        return result;
    }

    public TableMeta getTableMeta(String schema, String table) {
        return getTableMeta(schema, table, true);
    }

    public TableMeta getTableMeta(String schema, String table, boolean useCache) {
        if (!useCache) {
            tableMetaDB.invalidate(getFullName(schema, table));
        }

        return tableMetaDB.getUnchecked(getFullName(schema, table));
    }

    public TableMeta getTableMeta(String schema, String table, EntryPosition position) {
        return getTableMeta(schema, table, true, position);
    }

    // 怎么看着这里的逻辑是查的当前表结构信息？而不是position对应的表结构信息？
    // 确实，这里只是查当前的表结构，因为只要在运行，那么是按照顺序处理binlog的，DML用到的表结构信息都是当前的，DDL过来会自动维护进去，也就不存在需要找历史表结构的场景
    public synchronized TableMeta getTableMeta(String schema, String table, boolean useCache, EntryPosition position) {
        TableMeta tableMeta = null;
        if (tableMetaTSDB != null) {
            tableMeta = tableMetaTSDB.find(schema, table);
            if (tableMeta == null) {
                // 因为条件变化，可能第一次的tableMeta没取到，需要从db获取一次，并记录到snapshot中
                String fullName = getFullName(schema, table);
                ResultSetPacket packet = null;
                String createDDL = null;
                try {
                    try {
                        packet = connection.query("show create table " + fullName);
                    } catch (Exception e) {
                        // 尝试做一次retry操作
                        connection.reconnect();
                        packet = connection.query("show create table " + fullName);
                    }
                    if (packet.getFieldValues().size() > 0) {
                        createDDL = packet.getFieldValues().get(1);
                    }
                    // 强制覆盖掉内存值
                    tableMetaTSDB.apply(position, schema, createDDL, "first");
                    tableMeta = tableMetaTSDB.find(schema, table);
                } catch (IOException e) {
                    throw new CanalParseException("fetch failed by table meta:" + fullName, e);
                }
            }
            return tableMeta;
        } else {
            if (!useCache) {
                tableMetaDB.invalidate(getFullName(schema, table));
            }

            return tableMetaDB.getUnchecked(getFullName(schema, table));
        }
    }

    public void clearTableMeta(String schema, String table) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidate(getFullName(schema, table));
        }
    }

    public void clearTableMetaWithSchemaName(String schema) {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            for (String name : tableMetaDB.asMap().keySet()) {
                if (StringUtils.startsWithIgnoreCase(name, schema + ".")) {
                    // removeNames.add(name);
                    tableMetaDB.invalidate(name);
                }
            }
        }
    }

    public void clearTableMeta() {
        if (tableMetaTSDB != null) {
            // tsdb不需要做,会基于ddl sql自动清理
        } else {
            tableMetaDB.invalidateAll();
        }
    }

    /**
     * 更新一下本地的表结构内存
     *
     * @param position
     * @param schema
     * @param ddl
     * @return
     */
    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        if (tableMetaTSDB != null) {
            return tableMetaTSDB.apply(position, schema, ddl, extra);
        } else {
            // ignore
            return true;
        }
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
            .append(schema)
            .append('`')
            .append('.')
            .append('`')
            .append(table)
            .append('`')
            .toString();
    }


    public boolean isOnTSDB() {
        return isOnTSDB;
    }

    public void setOnTSDB(boolean isOnTSDB) {
        this.isOnTSDB = isOnTSDB;
    }

    public boolean isOnRDS() {
        return isOnRDS;
    }

    public void setOnRDS(boolean isOnRDS) {
        this.isOnRDS = isOnRDS;
    }

    public static void main(String[] args) {
        System.out.println(extractSchemaAndTableName("`zabbix7.6`.`test`"));
        System.out.println(extractSchemaAndTableName("zabbix7.6.`test`"));
        System.out.println(extractSchemaAndTableName("`zabbix7.6`.test"));
        System.out.println(extractSchemaAndTableName("zabbix.`test`"));

    }
}
