
-- SQL 

-- Add the jar 

-- add jar /home/sbouguerra/tez-autobuild/dist/hive/lib/kafka-handler-3.1.0-SNAPSHOT.jar;

drop table wiki_kafka_hive;

CREATE EXTERNAL TABLE wiki_kafka_hive
(`timestamp` timestamp , `channel` string,`page` string,`flags` string, `diffurl` string ,`user` string,
 `namespace` string,`isminor` boolean, 
`isnew` boolean, `isunpatrolled` boolean, `anonymous` boolean, `isrobot` boolean, added bigint, deleted int, 
delta bigint, `comment` string, `commentlength` int, `deltabucket` double)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "wiki-hive-topic", -- mandatory parameter
"kafka.bootstrap.servers"="cn105-10.l42scl.hortonworks.com:9092", -- mandatory parameter 
"kafka.serde.class"="org.apache.hadoop.hive.kafka.KafkaJsonSerDe" -- optional
);

-- WE support the following SERDE 
-- org.apache.hadoop.hive.serde2.JsonSerDe
-- org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
-- org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe
-- org.apache.hadoop.hive.serde2.avro.AvroSerDe
-- org.apache.hadoop.hive.serde2.OpenCSVSerde

ALTER TABLE wiki_kafka_hive SET TBLPROPERTIES ("kafka.serde.class"="org.apache.hadoop.hive.serde2.JsonSerDe");


-- describe list partitions and current offsets

Describe extended wiki_kafka_hive;

-- Simple count 

select count(*) from wiki_kafka_hive;

-- count the records of last 10 minutes

select count(*) from wiki_kafka_hive 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '10' MINUTES) ; 

-- Offset filter pushdown in action

select count(*)  from wiki_kafka_hive 
where (`__offset` < 10 and `__offset`>3 and `__partition` = 0) 
or (`__partition` = 0 and `__offset` < 105 and `__offset` > 99) 
or (`__offset` = 109);

-- shows the actual bounds 

select distinct `__start_offset`, `__end_offset`, `__partition` from wiki_kafka_hive 
where (`__offset` < 10 and `__offset`>3 and `__partition` = 0) 
or (`__partition` = 0 and `__offset` < 105 and `__offset` > 99) 
or (`__offset` = 109);


-- count the number of distict ppls 

select count(distinct `user`) as dist_count from wiki_kafka_hive 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '5' MINUTES)  
and wiki_kafka_hive.isnew = true ; 

-- Create Virtule view to enable PID masking or filter push down on time column
drop view l15min_wiki;

CREATE VIEW l15min_wiki as select * from wiki_kafka_hive 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '15' MINUTES) ; 

-- Create sub view will less fields
drop view l1h_wiki;

CREATE VIEW l1h_wiki as select  `timestamp`, `user`, `isrobot`, delta, added from wiki_kafka_hive 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '1' HOUR) ; 

-- most active user 
select `user`, sum(added) as added from l15min_wiki 
where `isrobot` = false group by `user` order by added desc limit 10;

-- most active bot
select `user`, sum(added) as added from l15min_wiki 
where `isrobot` = true group by `user` order by added desc limit 10;

-- top 10 channels 
select `channel`, sum(delta) as delta from l15min_wiki group by  `channel` order by delta desc limit 10;

-- top 10 edited pages last 10 min
select `channel`, `diffurl`, `page`,`namespace`, sum(delta) as delta from l15min_wiki 
group by `page`, `namespace`, `diffurl`, `channel` order by delta desc limit 10;

-- more drill down using  channel 
select `channel`, `diffurl`, `page`,`namespace`, sum(delta) as d from l15min_wiki
where `channel` = "#fr.wikipedia" 
group by `page`, `namespace`, `diffurl`, `channel` order by d desc limit 10;


-- Top 10 longest comments 
select `page`, max(commentlength) as l from l15min_wiki group by `page` order by l desc limit 10;

-- over close

select `channel`, `namespace`, `page`, `timestamp`, 
avg(delta) over (order by `timestamp` asc rows between  60 preceding and current row) as avg_delta,
 null, null, -1, -1,-1, -1 from l15min_wiki;




-- add data to orc table 
drop table user_table;
create table user_table (`user` string, `first_name` string , age int, gender string, comments string) STORED as ORC ;

select distinct `user`  as `user` , page as `first_name` , floor(1000/rand())%100 as age ,  case when (rand() > 0.5) then 'M' else 'F' end as gender , comment  
from l15min_wiki limit 10;


-- added by gender
select sum(added) as added, sum(deleted) as deleted, avg(delta) as delta, avg(age) as avg_age , gender 
from l15min_wiki  join user_table on `l15min_wiki`.`user` = `user_table`.`user` group by gender limit 10;




-- Strem join over the view it self 
-- FROM https://www.periscopedata.com/blog/how-to-calculate-cohort-retention-in-sql

select  count( distinct activity.`user`) as active_users, count(distinct future_activity.`user`) as retained_users
from l15min_wiki as activity
left join l15min_wiki as future_activity on
  activity.`user` = future_activity.`user`
  and activity.`timestamp` = future_activity.`timestamp` - interval '5' minutes ; 

--  Stream to stream join
select floor_hour(activity.`timestamp`), count( distinct activity.`user`) as active_users, count(distinct future_activity.`user`) as retained_users
from wiki_kafka_hive as activity
left join wiki_kafka_hive as future_activity on
  activity.`user` = future_activity.`user`
  and activity.`timestamp` = future_activity.`timestamp` - interval '1' hour group by floor_hour(activity.`timestamp`); 

-- INSERT INTO

drop table moving_avg_wiki_kafka_hive;
CREATE EXTERNAL TABLE moving_avg_wiki_kafka_hive 
(`channel` string, `namespace` string,`page` string, `timestamp` timestamp , avg_delta double )
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "moving_avg_wiki_kafka_hive_2",
"kafka.bootstrap.servers"="cn105-10.l42scl.hortonworks.com:9092",
-- STORE AS AVRO IN KAFKA
"kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe");

-- Insert data into the table;

insert into table moving_avg_wiki_kafka_hive select `channel`, `namespace`, `page`, `timestamp`, 
	avg(delta) over (order by `timestamp` asc rows between  60 preceding and current row) as avg_delta, 
	null as `__key`, null as `__partition`, -1, -1,-1, -1 from l15min_wiki;



-- ETL PIPE LINE

-- load form Kafka every Recod Exactly once 
-- Goal is to read data and commit both data and its offsets in a single Transaction 

-- The offset table 
Drop table kafka_table_offsets;
create table kafka_table_offsets(partition_id int, max_offset bigint, insert_time timestamp);

-- init 
insert overwrite table kafka_table_offsets select `__partition`, min(`__offset`) - 1, CURRENT_TIMESTAMP 
from wiki_kafka_hive group by `__partition`, CURRENT_TIMESTAMP ;

-- DW table

Drop table orc_kafka_table;
Create table orc_kafka_table (partition_id int, koffset bigint, ktimestamp bigint,
 `timestamp` timestamp , `page` string, `user` string, `diffurl` string, 
 `isrobot` boolean, added int, deleted int, delta bigint
) stored as ORC;

-- insert up to offset = 2 only

From wiki_kafka_hive ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id 
and ktable.`__offset` > offset_table.max_offset and  ktable.`__offset` < 3 )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`timestamp`, `page`, `user`, `diffurl`, `isrobot`, added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;

-- inserts only up to 2 
select max(`koffset`) from orc_kafka_table limit 10;

-- check no overllap

select count(*) as c  from orc_kafka_table group by partition_id, koffset having c > 1;

-- insert all 

From wiki_kafka_hive ktable JOIN kafka_table_offsets offset_table
on (ktable.`__partition` = offset_table.partition_id 
and ktable.`__offset` > offset_table.max_offset )
insert into table orc_kafka_table select `__partition`, `__offset`, `__timestamp`,
`timestamp`, `page`, `user`, `diffurl`, `isrobot`, added , deleted , delta
Insert overwrite table kafka_table_offsets select
`__partition`, max(`__offset`), CURRENT_TIMESTAMP group by `__partition`, CURRENT_TIMESTAMP;

-- current count
 select count(*) from (select distinct partition_id, koffset from orc_kafka_table) as src;

 select count(*) as c  from orc_kafka_table group by partition_id, koffset having c > 1;

