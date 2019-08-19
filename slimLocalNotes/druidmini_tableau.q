CREATE TABLE orc_calcs (
      key STRING,
      num0 DOUBLE,
      num1 DOUBLE,
      num2 DOUBLE,
      num3 DOUBLE,
      num4 DOUBLE,
      str0 STRING,
      str1 STRING,
      str2 STRING,
      str3 STRING,
      int0 INT,
      int1 INT,
      int2 INT,
      int3 INT,
      bool0 BOOLEAN,
      bool1 BOOLEAN,
      bool2 BOOLEAN,
      bool3 BOOLEAN,
      date0 STRING,
      date1 STRING,
      date2 STRING,
      date3 STRING,
      time0 STRING,
      time1 STRING,
      datetime0 STRING,
      datetime1 STRING,
      zzz STRING
)
STORED AS orc tblproperties ("orc.compress"="SNAPPY");

create table calcs
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
  "druid.segment.granularity" = "MONTH",
  "druid.query.granularity" = "DAY")
AS SELECT
      cast(datetime0 as timestamp with local time zone) `__time`,
      key,
      str0, str1, str2, str3,
      date0, date1, date2, date3,
      time0, time1,
      datetime0, datetime1,
      zzz,
      cast(bool0 as string) bool0,
      cast(bool1 as string) bool1,
      cast(bool2 as string) bool2,
      cast(bool3 as string) bool3,
      int0, int1, int2, int3,
      num0, num1, num2, num3, num4
from orc_calcs;


EXPLAIN SELECT `Calcs`.`key` AS `none_key_nk`,   (`Calcs`.`num4` IS NULL) AS `none_z_isnull_num_nk`,   `Calcs`.`num4` AS `sum_num4_ok` FROM  `calcs` `Calcs`;

