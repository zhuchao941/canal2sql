# canal2sql

用Java写的 MySQL Binlog 解析工具，底层依赖了Canal

## 设计初衷

1. 为了方便的找到数据的更新轨迹
2. 为了填补Java在解析离线binlog的空白

### binlog读取模式

暂时支持了三种读取模式

- 在线模式（online）：指定远程数据库地址，实时从对应数据库获取binlog并解析
- binlog文件模式（file）：离线模式，指定binlog文件url（支持http/file等），读取对应文件并解析
- 阿里云rds模式（aliyun）：离线模式，指定对应的时间范围以及实例id（instanceId），读取符合条件的binlog文件列表并解析

### 列补全方式

我们知道binlog里row_event和table_map是没有字段名的，所以需要借助外部元数据来补全，canal2sql支持两种列补全方式

- 本地DDL文件：指定本地DDL文件，来获取表结构数据，从而补全binlog里的列信息
- 数据库实时DDL：指定数据库地址信息，直接用实时表结构来补全binlog里的列信息

在线模式只支持使用数据库实时DDL的方式，而binlog文件模式和阿里云rds模式则可以根据实际情况选择合适的列补全方式

### 限制和要求

- MySQL必须开启binlog
- binlog_format = row
- binlog_row_image = full
- 如果用在线解析binlog，对应用户要有SELECT, REPLICATION SLAVE, REPLICATION CLIENT权限

## 可执行jar包下载

最新版本v1.1.0 可在releases页面下载 
https://github.com/zhuchao941/canal2sql/releases/tag/v1.1.0

## 运行选项

**binlog模式指定**

- -mode online/file/aliyun，默认online

**SQL显示模式**

- -A, --append   追加模式，支持原SQL和回滚SQL同时显示，可选，默认true
- -B, --rollback 显示回滚SQL（如在追加模式，则原SQL也会显示，作为注释添加在同一行），可选，默认false
- -M, --minimal  最小化insert/delete语句，如原语句是batchInsert和batchDelete的，那么会尽量按照原批量语句输出，可选， 默认false（v1.1.0版本新增）

**解析范围控制**

- -start_position 起始解析位置。可选。
  - 对于在线模式，需要指定起始文件+position，如'mysql-bin.007756|52416008'
  - 对于离线模式，只有position

- -end_position 终止解析位置。可选
  - 对于在线模式，需要指定终止文件+position，如'mysql-bin.007756|52416008'
  - 对于离线模式，只有position

- -start_time 起始解析时间，格式'yyyy-MM-dd HH:mm:ss'。可选

- -end_time 终止解析时间，格式'yyyy-MM-dd HH:mm:ss'。可选

**对象过滤**

- -filter 库表维度过滤（白名单），如：a库下所有表a.*，多个规则英文逗号(,)隔开

- -black_filter 库表维度过滤（黑名单），语法和filter一致

- -sql-type 只解析指定类型，支持 insert,update,delete,ddl。多个类型用逗号隔开，如--sql-type=insert,delete。可选。默认为insert,update,delete,ddl

**离线解析模式**

- -ddl 本地DDL文件，用来还原列
- -file_url 离线binlog文件，支持各种协议，填入可访问url即可

**mysql连接配置**

在线解析必填，离线模式作为表结构来源用来做列还原

```
 -h host
 -P port
 -u user
 -p password
```

**云rds配置**

- -instanceId 云数据库实例instanceId，阿里云是主从库公用一个instanceId，所以还要指定对应的编号，用"|"隔开，如'rm-bp13bt768ubdz4eb2|26588881'
- -ak AccessKey
- -sk SecretKey
- -external 公网模式，公网下载binlog要收费，而内网下载binlog不收钱且速度更快，默认为内网模式
- -start_time 起始解析时间，格式'yyyy-MM-dd HH:mm:ss'。此模式下必传
- -end_time 终止解析时间，格式'yyyy-MM-dd HH:mm:ss'。此模式下必传

## 使用示例

### 1. 在线模式
```sh
java -jar ./canal2sql-${version}.jar -mode online -uroot -proot -P3306 -hlocalhost
```

在线解析模式只需要指定远程数据库连接参数即可，表结构数据直接实时获取

### 2. binlog文件模式

binlog文件模式需要指定binlog文件url，表结构数据支持从mysql实时获取也支持从本地ddl文件读取

```sh
java -jar ./canal2sql-${version}.jar -mode file -file_url 'file:/Users/abc/binlog/mysql-bin.000474' -uroot -proot -P3306 -hlocalhost
java -jar ./canal2sql-${version}.jar -mode file -ddl '/Users/xxx/ddl.sql' -file_url 'http://localhost:8080/binlog/mysql-bin.000474'
```

### 3. 阿里云rds模式

阿里云rds模式

```
java -jar ./canal2sql-${version}.jar -mode aliyun -uroot -proot -P3306 -hlocalhost -start_time "2023-12-07 00:00:00" -end_time "2023-12-07 09:00:00" -instanceId "rm-12345678|1111111" -ak ak -sk sk
```

## 输出格式解析

类似如下输出，第一行为 binlog文件名称:事务开始的offset 时间戳
后面就是具体的事务里的sql语句，同一个事务会被聚合到一起输出

```sql
#mysql-bin.007795:171260617 2023-12-14 16:27:00
UPDATE `comment`.`comment_category` SET `upd_tm` = '2023-12-14 16:27:00' WHERE `id` = 1;
UPDATE `comment`.`comment_category` SET `upd_tm` = '2023-12-14 16:27:00' WHERE `id` = 12;

#mysql-bin.007795:171261166 2023-12-14 16:27:00
UPDATE `comment`.`comment_category` SET `upd_tm` = '2023-12-14 16:27:00' WHERE `id` = 9;
```

## RoadMap

1. ~~支持binlog的在线解析、离线解析~~（已支持）
2. ~~列还原同时支持在线模式和离线模式~~（已支持）
3. ~~支持生成回滚SQL~~（已支持）
4. ~~支持解析范围控制~~（已支持）
    1. ~~时间维度~~
    2. ~~position维度~~
5. ~~支持对象过滤~~（已支持）
    1. ~~库表维度~~
    2. ~~sql类型维度~~
6. 支持多binlog文件顺序解析
7. ~~支持云rds binlog文件列表解析~~（已支持）
8. 支持分析统计功能
    1. 耗时事务统计
    2. 大事务统计
    3. 表维度/库维度 更新数据量统计
    4. qps统计

暂时只剩下一些分析统计相关的功能未实现，后续有时间会考虑实现。

另外对于命令行参数的一些校验也没做，这块后续也可以优化

## 致谢
    canal2sql借鉴和学习了很多业界知名的开源项目，在此表示感谢！
- [canal](https://github.com/alibaba/canal)
- [bingo2sql](https://github.com/hanchuanchuan/bingo2sql)
- [binlog2sql](https://github.com/danfengcao/binlog2sql)