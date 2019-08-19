DROP TABLE kafka_table_avro_12_sep;
CREATE EXTERNAL TABLE kafka_table_avro_12_sep 
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES
("kafka.topic" = "test-avro-write-12-sep-2",
"kafka.bootstrap.servers"="cn105-10.l42scl.hortonworks.com:9092",
"kafka.serde.class"="org.apache.hadoop.hive.serde2.avro.AvroSerDe",
'avro.schema.literal'='{
  "type" : "record",
  "name" : "Wikipedia",
  "namespace" : "com.slim.kafka",
  "version": "1",
  "fields" : [ {
    "name" : "count",
    "type" : "string"
  }, {
    "name" : "userid",
    "type" : "string"
  } ]
}'
);


insert into table kafka_table_avro_12_sep (`count`,`userid`,`__key`, `__partition`, `__offset`, `__timestamp`, `__start_offset`, `__end_offset`)
values ("10","user-test",null,null ,-1,-1,-1,null );


ALTER TABLE kafka_table_avro_12_sep SET TBLPROPERTIES ("kafka.consumer.isolation.level"="read_committed");

select count(*) from kafka_table_avro 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '1' HOURS) ; 


select count(*), `__start_offset`, `__end_offset` from kafka_table_avro 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '5' MINUTES) 
and kafka_table_avro.isnew = true and kafka_table_avro.isminor = true group by `user`, `__start_offset`, `__end_offset` ; 


select count(*) from kafka_table_avro 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESAMP - interval '15' MINUTES) ;


select `user`, count(*) from kafka_table_avro 
where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESAMP - interval '15' MINUTES) 
and kafka_table_avro.isnew = true and kafka_table_avro.isminor = true group by `user`;


select count(*) , `user` from kafka_table_avro where `__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '15' MINUTES) and kafka_table_avro.isnew = true and kafka_table_avro.isminor = true group by `user` ; 


insert into table kafka_table_avro_12_sep (`count`,`userid`,`__key`, `__partition`, `__offset`, `__timestamp`, `__start_offset`, `__end_offset`) 
select cast(count(*) as string) as `count` , `user` as `user`, NULL as `__key`, -1 as `__partition`, -1 as `__offset`,
 -1 as `__timestamp`, -1 as `__start_offset`, -1 as `__end_offset` from kafka_table_avro where 
`__timestamp` >  1000 * to_unix_timestamp(CURRENT_TIMESTAMP - interval '30' MINUTES) 
and kafka_table_avro.isnew = true and kafka_table_avro.isminor = true group by `user` ; 



select count(*) from kafka_table_avro_12_sep join kafka_table_avro on kafka_table_avro_12_sep.userid = kafka_table_avro.`user`; 