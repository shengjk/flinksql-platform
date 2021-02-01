## 项目现状

#### 环境

```
jdk8
flink1.12.x
```

#### 最终达到的效果

```
传入 sql 文件，即可执行，然后通过命令行提交
```

#### 设置参数

```
可以在 sql 文件中设置 checkpointing.interval、state.ttl 等，具体可以查看 sqldemo/sqlDemo.sql
目前支持设置 ExecutionConfigOptions、OptimizerConfigOptions、TableConfigOptions、ExecutionCheckpointingOptions 类中的参数

对于 Duration 格式：
Parse the given string to a java {@link Duration}. The string is in format "{length
value}{time unit label}", e.g. "123ms", "321 s". If no time unit label is specified, it will
be considered as milliseconds.

<p>Supported time unit labels are:

<ul>
  <li>DAYS： "d", "day"
  <li>HOURS： "h", "hour"
  <li>MINUTES： "min", "minute"
  <li>SECONDS： "s", "sec", "second"
  <li>MILLISECONDS： "ms", "milli", "millisecond"
  <li>MICROSECONDS： "µs", "micro", "microsecond"
  <li>NANOSECONDS： "ns", "nano", "nanosecond"
```

#### 部署

```
除了必要的 connector 还需要下面的 jar
kafka-clients-2.4.1.jar
hive-exec-1.2.1.jar
hive-metastore-1.2.1.jar
libfb303-0.9.2.jar
```

#### 执行方式

```
flink-1.12.0/bin/flink  run -p 3 -yt ./flinkjar/  -C file:///home/shengjk/flinkjar/test-udf.jar -C file:///home/shengjk/flinkjar/jedis-2.10.2.jar  -m yarn-cluster -ynm sqlDemo  -c io.github.shengjk.Main ./flinksql-platform-1.0-SNAPSHOT.jar --sqlPath ./sqlDemo.sql
or
flink-1.12.0/bin/flink  run -p 3 -yt ./flinkjar/  -C file:///home/shengjk/flinkjar/test-udf.jar -C file:///home/shengjk/flinkjar/jedis-2.10.2.jar  -m yarn-cluster -ynm sqlDemo  -c io.github.shengjk.Main ./flinksql-platform-1.0-SNAPSHOT.jar --sqlPath hdfs://nameservice1/sqlDemo.sql

-C 添加 udfJar 等第三方 jar 包  -C 参数apply到了client端生成的JobGraph里，然后提交JobGraph来运行的 
-yt 目录 将 udfJar 等第三方 jar 包提交到 TaskManager 上

sql 文件默认的注释方式为 -- 若要改变注释方式通过参数  --comment
```

## other

#### 自定义的 RedisSink

```
create table test(
`id` bigint,
 `url` string,
 `day` string,
  `pv` long,
  `uv` long
) with {
    'connector'='redis',
    'hostPort'='xxx',
    'password'='',
    'expireTime'='100',
    'keyType'='hash',
    'keyTemplate'='test_${id}',
    'fieldTemplate'='${day}',
    'valueNames'='pv,uv',
}

假设 id=1 day=20201016 pv=20,uv=20
redis result: 
    test_1 20201016-pv 20,20201016-uv 20

参数解释：
connector  固定写法
hostPort   redis 的地址
password   redis 的密码
expireTime  redis key 过期时间，单位为 s
keyType  redis key 的类型，目前有 hash、set、sadd、zadd
keyTemplate  redis key 的表达式，如 test_${id} 注意 id 为表的字段名
fieldTemplate  redis keyType==hash 时，此选项为必选，表达式规则同 keyTemplate
valueNames  redis value  only 可以有多个

keyTemplate、fieldTemplate、valueNames 对应的值如果为 null 则转为 "null"
```

#### 自定义 canal json 解析

```
对应 canal json 格式为

insert operator:
{
        "sourceType": "MYSQL",
        "eventType": "INSERT",
        "schemaName": "example",
        "tableName": "test1",
        "serverId": 1,
        "eventLength": 44,
        "logfileName": "mysql-bin.000019",
        "logfileOffset": 10219,
        "isDdl": false,
        "ddlSql": null,
        "rowData": {
            "beforeRow": null,
            "afterRow": "{\"name\":\"33\",\"id\":\"33\"}"
        }
    }
        
    {
            "sourceType": "MYSQL",
            "eventType": "DELETE",
            "schemaName": "example",
            "tableName": "test1",
            "serverId": 1,
            "eventLength": 159,
            "logfileName": "mysql-bin.000019",
            "logfileOffset": 11687,
            "isDdl": false,
            "ddlSql": null,
            "rowData": {
                    "beforeRow": "{\"name\":\"33\",\"id\":\"33\"}",
                    "afterRow": null
                }
        }
        
    {
        "sourceType": "MYSQL",
        "eventType": "UPDATE",
        "schemaName": "example",
        "tableName": "test1",
        "serverId": 1,
        "eventLength": 62,
        "logfileName": "mysql-bin.000012",
        "logfileOffset": 3738,
        "isDdl": false,
        "ddlSql": null,
        "rowData": {
            "beforeRow": "{\"name\":\"q11111\",\"id\":\"11\"}",
            "afterRow": "{\"name\":\"q11111\",\"id\":\"1\"}"
        }
    }

```

## 欢迎提 PR 和 issue