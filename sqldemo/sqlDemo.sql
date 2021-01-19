CREATE TEMPORARY FUNCTION MillisecondsToDateStr AS 'io.github.shengjk.udf.MillisecondsToDateStr' LANGUAGE JAVA;
--
-- 设置 tableEnv 配置，否则的话则走默认配置;
-- configOptions.putAll(extractConfigOptions(ExecutionConfigOptions.class));
-- configOptions.putAll(extractConfigOptions(OptimizerConfigOptions.class));
-- configOptions.putAll(extractConfigOptions(TableConfigOptions.class));
-- configOptions.putAll(extractConfigOptions(ExecutionCheckpointingOptions.class));
-- hive DDL,DQL(HQL) set table.sql-dialect=hive; -- to use hive dialect
-- other set table.sql-dialect=default; -- to use default dialect

-- ExecutionCheckpointingOptions
set execution.checkpointing.mode=EXACTLY_ONCE;
set execution.checkpointing.timeout=30 min;--  30min
-- set execution.checkpointing.max-concurrent-checkpoints=2;
-- set execution.checkpointing.min-pause=60000;  -- 1min
set execution.checkpointing.interval=1 min ; -- 1min
set execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION;
-- set execution.checkpointing.prefer-checkpoint-for-recovery=true;
-- set execution.checkpointing.tolerable-failed-checkpoints=1;
-- set execution.checkpointing.unaligned=true;
-- set execution.checkpointing.alignment-timeout=60; -- second
-- set execution.checkpointing.unaligned.forced=true;

-- ExecutionConfigOptions
set table.exec.state.ttl=1 day;  -- 1 day
set table.exec.mini-batch.enabled=true; -- enable mini-batch optimization
set table.exec.mini-batch.allow-latency=1 s; -- 1s
set table.exec.mini-batch.size=1000;
set table.exec.sink.not-null-enforcer=drop;

-- OptimizerConfigOptions


-- hive
set table.sql-dialect=hive; -- to use hive dialect
set table.sql-dialect=default; -- to use default dialect
--
CREATE CATALOG myhive WITH (
    'type' = 'hive',
--     'default-database' = 'mydatabase',
--  原生的这个是必须配置的(此版本可以通过 env 找到)， hadoop-conf-dir 可以通过 env 找到
    'hive-conf-dir' = '/hive-conf'
);
-- set the HiveCatalog as the current catalog of the session
USE CATALOG myhive;

SHOW CATALOGS;
SHOW DATABASES;
SHOW TABLES;
SHOW CURRENT CATALOG;  -- default default_catalog
SHOW CURRENT DATABASE;  -- default default

-- 不能通过 default.aggregated_buildings 来访问
select * from  aggregated_buildings limit 10;

use CATALOG default_catalog;
use database default;
--
-- -- dadadadadada
CREATE TABLE orders
(
    status      int,
    courier_id  bigint,
    id          bigint,
    finish_time BIGINT
)
WITH (
    'connector' = 'kafka','topic' = 'canal_monitor_order',
    'properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup',
    'format' = 'ss-canal-json','ss-canal-json.table.include' = 'orders','scan.startup.mode' = 'earliest-offset');

-- flink.partition-discovery.interval-millis;
CREATE TABLE infos
(
    info_index int,
    order_id   bigint
)
WITH (
    'connector' = 'kafka','topic' = 'canal_monitor_order',
    'properties.bootstrap.servers' = 'localhost:9092','properties.group.id' = 'testGroup',
    'format' = 'ss-canal-json','ss-canal-json.table.include' = 'infos','scan.startup.mode' = 'earliest-offset');


CREATE TABLE redisCache
(
    finishOrders BIGINT,
    courier_id   BIGINT,
    dayStr       String
)
WITH (
    'connector' = 'redis',
    'hostPort'='localhost:6400',
    'keyType'='hash',
    'keyTemplate'='test2_${courier_id}',
    'fieldTemplate'='${dayStr}',
    'valueNames'='finishOrders',
    'expireTime'='259200');

create view temp as
select o.courier_id,
       (CASE
            WHEN sum(infosMaxIndex.info_index) is null then 0
            else sum(infosMaxIndex.info_index) end) finishOrders,
       o.status,
       dayStr
from ((select courier_id,
              id,
              last_value(status)                             status,
              MillisecondsToDateStr(finish_time, 'yyyyMMdd') dayStr
       from orders
       where status = 60
       group by courier_id, id, MillisecondsToDateStr(finish_time, 'yyyyMMdd'))) o
         left join (select max(info_index) info_index, order_id
                    from infos
                    group by order_id) infosMaxIndex on o.id = infosMaxIndex.order_id
group by o.courier_id, o.status, dayStr;


INSERT INTO redisCache SELECT finishOrders,courier_id,dayStr FROM temp;
