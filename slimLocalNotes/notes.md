Druing the last couple of days i have tried understand the code.
What i found out is 
The Test File that starts the call is org.apache.hadoop.hive.cli.TestMiniDruidCliDriver
This file will set the fields nature of test cluster via org.apache.hadoop.hive.cli.control.CliConfigs.MiniDruidCliConfig.
The actual call to construct the Druid Cluster is done at org.apache.hadoop.hive.ql.QTestUtil. 
	More sepcific it is done at this function org.apache.hadoop.hive.ql.QTestUtil#setupMiniCluster
```java
	} else if (clusterType == MiniClusterType.druid) {
      druidCluster = MiniDruidCluster
              .createDruidCluster("Druid-Cluster", String.format("localhost:%d", setup.zooKeeperCluster.getClientPort()) , new File(getLogDirectory()));
      druidCluster.init(new Configuration());
      druidCluster.start();
    }
```
What i need to design is to first decide where the code of mini druid cluster will be living.
Then need to find out if i want to do it as processes running within different JVM or use the same Thread.
Find out how to test this maybe?
See how that will work with Hive?

The inputs of druid Process will be the followings 
	logDir
	segmentsDir
	workingDir
	derby
	hadoopConfig
	working ZkPort


HOW to convert Hive operators to Calcite ones look at function org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter#getCalciteOperator(java.lang.String, org.apache.hadoop.hive.ql.udf.generic.GenericUDF, com.google.common.collect.ImmutableList<org.apache.calcite.rel.type.RelDataType>, org.apache.calcite.rel.type.RelDataType)

Also this static block org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.StaticBlockBuilder#StaticBlockBuilder

To register new mapping use this function org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter.StaticBlockBuilder#registerFunction

