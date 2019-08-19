
create table test_base_table(`timecolumn` timestamp, `date_c` string, `timestamp_c` string,  `metric_c` double);

insert into test_base_table values ('2015-03-08 00:00:00', '2015-03-10', '2015-03-08 05:30:20', 5.0);
insert into test_base_table values ('2016-03-08 00:00:00', '2015-03-10', '2015-03-08 05:30:20', 2.0);
insert into test_base_table values ('2014-04-05 00:00:00', '2015-03-10', '2015-03-08 05:30:20', 3.0);
-- insert into test_base_table values ('2015-03-08 00:00:00', 'i1-start', 4, 'UTC', 'dim');
-- insert into test_base_table values ('2015-03-08 23:59:59', 'i1-end', 1 , 'UTC', 'dim');
-- insert into test_base_table values ('2015-03-09 00:00:00', 'i2-start', 4, 'UTC', 'dim');
-- insert into test_base_table values ('2015-03-09 23:59:59', 'i2-end', 1, 'UTC', 'dim');
--insert into test_base_table values ('2017-03-10 00:00:00', 'i3-start', 2, '2004-08-02 07:59:23', 'dim');
--insert into test_base_table values ('2015-03-10 23:59:59', 'i3-end', 2, '2015-03-10', 'dim');

--select cast(`timecolumn` as timestamp with local time zone) as `__time`, `interval_marker`, `num_l` from test_base_table;
CREATE TABLE druid_test_table
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "DAY")
AS select
cast(`timecolumn` as timestamp with local time zone) as `__time`, `date_c`, `timestamp_c`, `metric_c` FROM test_base_table;

Explain SELECT `Calcs`.`date_c` AS `none_key_nk`,   (`Calcs`.`metric_c` IS NULL) AS `none_z_isnull_num_nk`,   `Calcs`.`metric_c` AS `sum_num4_ok` FROM `druid_test_table` `Calcs` ;

SELECT `Calcs`.`date_c` AS `none_key_nk`,   (`Calcs`.`metric_c` IS NULL) AS `none_z_isnull_num_nk`,   `Calcs`.`metric_c` AS `sum_num4_ok` FROM `druid_test_table` `Calcs` ;

explain SELECT `druid_test_table`.`date_c` AS `none_key_nk`,   CAST(YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'))
- YEAR(FROM_UNIXTIME(1001, 'yyyy-MM-dd HH:mm:ss')) AS BIGINT) AS `sum_z_now_ok`
FROM druid_test_table;

SELECT `druid_test_table`.`date_c` AS `none_key_nk`,   CAST(YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss'))
- YEAR(FROM_UNIXTIME(1001, 'yyyy-MM-dd HH:mm:ss')) AS BIGINT) AS `sum_z_now_ok`
FROM druid_test_table;


explain select DATE_ADD(cast(`__time` as date), CAST(metric_c AS INT)) from druid_test_table;
select DATE_ADD(cast(`__time` as date), CAST(metric_c AS INT)) from druid_test_table;

explain select DATE_SUB(cast(`__time` as date), CAST(metric_c AS INT)) from druid_test_table;
select DATE_SUB(cast(`__time` as date), CAST(metric_c AS INT)) from druid_test_table;


explain select UNIX_TIMESTAMP(CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss') from druid_test_table;
select UNIX_TIMESTAMP (CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ) from druid_test_table;

explain select unix_timestamp(from_unixtime(1396681200)) from druid_test_table;
select unix_timestamp(from_unixtime(1396681200)) from druid_test_table;


explain select FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss') from druid_test_table;
select FROM_UNIXTIME(UNIX_TIMESTAMP (CAST(`__time` as timestamp ),'yyyy-MM-dd HH:mm:ss' ),'yyyy-MM-dd HH:mm:ss') from druid_test_table;

explain select FLOOR (`__time` TO YEAR), TRUNC(cast(`__time` as timestamp), 'YY') from druid_test_table;
select FLOOR (`__time` TO YEAR), TRUNC(cast(`__time` as timestamp), 'YY') from druid_test_table;
select FLOOR (`__time` TO YEAR), TRUNC(cast(`__time` as timestamp), 'YEAR') from druid_test_table;
select FLOOR (`__time` TO YEAR), TRUNC(cast(`__time` as timestamp), 'YYYY') from druid_test_table;

select TRUNC(cast(`timecolumn` as timestamp), 'YY') from test_base_table;

explain select FLOOR (`__time` TO MONTH), TRUNC(cast(`__time` as timestamp), 'MONTH') from druid_test_table;
select FLOOR (`__time` TO MONTH), TRUNC(cast(`__time` as timestamp), 'MONTH') from druid_test_table;
select FLOOR (`__time` TO MONTH), TRUNC(cast(`__time` as timestamp), 'MM') from druid_test_table;
select FLOOR (`__time` TO MONTH), TRUNC(cast(`__time` as timestamp), 'MON') from druid_test_table;
select TRUNC(cast(`timecolumn` as timestamp), 'MM') from test_base_table;

explain select TRUNC(cast(`__time` as timestamp), 'QUARTER') from druid_test_table;
select FLOOR (`__time` TO QUARTER), TRUNC(cast(`__time` as timestamp), 'QUARTER') from druid_test_table;
select FLOOR (`__time` TO QUARTER), TRUNC(cast(`__time` as timestamp), 'Q') from druid_test_table;
select TRUNC(cast(`timecolumn` as timestamp), 'Q') from test_base_table;

explain select TO_DATE(`__time`) from druid_test_table;

select TO_DATE(`__time`) from druid_test_table;

explain select TO_DATE(`__time`), TO_DATE(`date_c`) from druid_test_table;
select TO_DATE(`__time`), TO_DATE(`date_c`) from druid_test_table;

EXPLAIN SELECT SUM((`ssb_druid_100`.`metric_c` * `ssb_druid_100`.`metric_c`)) AS `sum_calculation_4998925219892510720_ok`,
  CAST(TRUNC(CAST(`ssb_druid_100`.`__time` AS TIMESTAMP),'MM') AS DATE) AS `tmn___time_ok`
FROM `default`.`druid_test_table` `ssb_druid_100`
GROUP BY CAST(TRUNC(CAST(`ssb_druid_100`.`__time` AS TIMESTAMP),'MM') AS DATE);

SELECT SUM((`ssb_druid_100`.`metric_c` * `ssb_druid_100`.`metric_c`)) AS `sum_calculation_4998925219892510720_ok`,
  CAST(TRUNC(CAST(`ssb_druid_100`.`__time` AS TIMESTAMP),'MM') AS DATE) AS `tmn___time_ok`
FROM `default`.`druid_test_table` `ssb_druid_100`
GROUP BY CAST(TRUNC(CAST(`ssb_druid_100`.`__time` AS TIMESTAMP),'MM') AS DATE);




-- SELECT CAST(`ssb_druid_100`.`__time` AS TIMESTAMP) AS `x_time`,
-- SUM(`ssb_druid_100`.`metric_c`) AS `sum_lo_revenue_ok`
-- FROM `default`.`druid_test_table` `ssb_druid_100`
-- GROUP BY CAST(`ssb_druid_100`.`__time` AS TIMESTAMP);




--SELECT CAST(`ssb_druid_100`.`__time` AS TIMESTAMP) AS `x_time`,
--SUM(`ssb_druid_100`.`metric_c`) AS `sum_lo_revenue_ok`
--FROM `default`.`druid_test_table` `ssb_druid_100`
--GROUP BY CAST(`ssb_druid_100`.`__time` AS TIMESTAMP);



--explain select
--year(date_c), month(date_c),day(date_c), hour(date_c),
--year(timestamp_c), month(timestamp_c),day(timestamp_c), hour(timestamp_c)
--from druid_test_table;

--select year(date_c), month(date_c),day(date_c), hour(date_c),
--year(timestamp_c), month(timestamp_c),day(timestamp_c), hour(timestamp_c)
--from druid_test_table;




--explain SELECT SUM(`num_l`) AS `sum_lo_revenue_ok` FROM druid_test_table GROUP BY 1.1000000000000001;

--SELECT SUM(`num_l`) AS `sum_lo_revenue_ok` FROM test_base_table GROUP BY 1.1000000000000001;

