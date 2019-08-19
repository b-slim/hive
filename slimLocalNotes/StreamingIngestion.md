Generaly speaking those are a requirements for streaming use case:
- No man left behind: Need to ensure that ever record is read-transformed-written once to the hive table even in the presence of failures. 

- Consistency: once the record is committed can not be subject to any intermitten absence, this implies if i query n times a given record i should see the same results. 

- Reliability: failues is a defacto player and we need to handle any kind fault/failure.

- Scalability/Throughput: need to be able to scale up/down or/and backoff depending on systems resources/status. 


The problem statment:
 Consumer is reading form stream starting form the last consumed msg marked at upstream as committed. 
 Once read the records is handed off to TransactionBatch writer.
 at this poit we want to make sure that record are written by TransactionBatch in a transcitional fashion both down stream and upstream. In pseudo-code this looks like this.

 KafkaConsumer consumer = inti();
 TransactionBatch txnBatch = init();

 While(txnBatch.hasLocks()) {
 	while(buffer.maxSize() < batch) {
 		buffer.add(consumer.poll())
 	}

 	txnBatch.beginNextTransaction();
 	try{ 
 		txnBatch.write(format(buffer));
 		// First Commit with HiveMetaStore
 		txnBatch.commit() ; // if the program dies her then fine nothing is marked as read
 		
 		//Then commit last consumed msg
 		consumer.commit(buffer.getStartOffset(),buffer.getLastOffset()); // The question is, if the program dies here, would abort down at catch block be enough for rollback ?
 		
	} catch(KafkaCommitException) {
		txnBatch.abort();
	}

 }


 
# Introduction
The main goal of this work is to allow Hive users to be able to ingest Data from a streaming system eg Kafka, where all the interaction is done via Hive CLI.
The objective here is to have the decent ingestion rate with Hive only as a streaming ingestion system.
By decent we are targeting a Lag in the miniuts range.
In order to get there we need to build an ingestion pipeline between Hive and Kafka.
From a high level point of view the pipeline is effectively pulling data from Kafka using a kafka consumer, buffering the records localy and then once buffer fills up the pipe will flush to HDFS and update Hive metastore about the newlly pused data.

# Highlevel requirements
Such pipeline need to meet the following requieremnts:
- Pipeline has to run continously unless the user drop the table.
- Pipeline need to have pause/resume hooks where the user can chose to stop the streaming or restart form the last known offset.
- Pipeline need to be scalable, if we give it more resources, this means that we have to make sure that the pipeline will run in parallel streaming from various partition.
- Pipleline need to be realiable, this means we need to have a way to babysite the pipeline workers and restart failed ones/ kill hanging ones.. etc

# Solution 1 Using kafka Connect Cluster
## Section 1
This section describes Kafka connect in nutshell (more informations can be found here https://kafka.apache.org/documentation/#connect)
Kafka Connect is designed by Kafka community to move data to/from a Kafka Cluster.
Kafka Connect abstracts to the user how to push/pull data to/from Kafkam.
**Pros **
+ We don't have to deal with long running tasks (most of the bulk is done by KCluster).
+ Kafka Connect tracks offsets for each one so that connectors can resume from their previous position in the event of failures or graceful restarts for maintenance.
+ Kafka Connect cluster is actually isolating the load very well between HIVE and Kafka, eg if something wrong happens with Kcluster it will not take down Hive Cluster.
+ The code to push to HDFS is there already tested and well used. 
**Cons**
- Someone has to own this set of kafka connect works which means at minimum:
	- Start/stop those demons (this can be done via Yarn API or add it as Ambari service)
	- provide some configuration like security configs or kafka brokers addresses
	- restart the service if it stops

IMO this can be added as another HDP kafka service like the kafka brokers. FYI talked to Kafka guys and they said the bulk of the kafka connect code is already there.

## What need to be done:

- 1 Define the ownership and operation workflow of kafka workers. This can be adding new Ambari Service and let the user start and stop the nodes from there.
- 2 Importing and packaging of HDFS connector, currently this is a conculent owned product licenced as Apache V2
- 3 Extend HDFS connector to use streaming Hcat API.
- 4 Add to HiveServer2 appropiate hooks and callback to interact with kafka connect cluster.
- 2 Current HDFS connector supports parquet only we need to add ORC.



 CREATE TABLE kafka_example(userid BIGINT, page_url STRING)
 PARTITIONED BY(dt STRING, country STRING)
 CLUSTERED BY(userid) INTO 32 BUCKETS
 TBLPROPERTIES (
 //Mandatory properties
"kafka.topic" = "pageviews", 
"kafka.broker.list" = "localhost:8085",
//optional properties
"kafka.batch.maxSize" = "500000",
"kafka.max.batch.period.ms" = "900000", // 15 * 60 * 1000 (15 mins)
"kafka.max.num.tasks" = "5",
"kafka.schema.compatibility" = "some_property", // need to define schema evolutions
"kafka.read_uncommited" = "true",
"kafka.extra.propertie.key1" = "value" // extra properties to inject to kafka in case of we have to
"kafka.extra.propertie.key2" = "value1"
)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStreamStorageHandler';



 STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
"druid.segment.granularity" = "YEAR",
"druid.query.granularity" = "MINUTE"
)


# Solution 2 Do it your self

- 1 Add background thread like compactor to be the ingestion manager. Ingestion manager will be tracking ingestion 24/7. Ingestion manager has to keep track of what is read from kafka. 
- 2 Ingestion mangager will be submiting periodic MR tasks to read a given amount of kafka offsets and push that to HDFS via the streaming API or bare writes to HDFS.
- 3 Ingestion manager has to be a statfull node and will be keeping track of what need to be read and what has failed then re-submit the task if needed.
- 4 need to have a way to restart the ingestion manager thread in case it dies.
- 5 need to have a way to make the state of such manager portable in case it is run on a different node or we simply restart the metasotre. 

**Pros**
 - We do not need an external pipeline managment system like kafka connect

**Cons**
- We need to manage our self the kafka consumer process end to end that means tracking offsets/failures recovery/ scalabily
- We will be using Hive resources thus this can cause more congestions over hive cluster.








{
  "name": "hdfs-sink",
  "config": {
    "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
    "tasks.max": "5",
    "topic": "test_hdfs",
    "hdfs.url": "hdfs://localhost:9000",
    "flush.maxSize": "3",
    "name": "hdfs-sink",
    "hive.metastore.uris"="thrift://localhost:9083" # FQDN for the host part
	"schema.compatibility=BACKWARD"
    "format.class"="io.confluent.connect.hdfs.parquet.ParquetFormat"
	"partitioner.class"="io.confluent.connect.hdfs.partitioner.HourlyPartitioner"
	"hdfs.authentication.kerberos"=true
	"connect.hdfs.principal"="connect-hdfs/_HOST@YOUR-REALM.COM"
	"connect.hdfs.keytab"="path to the connector keytab"
	"hdfs.namenode.principal"="namenode principal"
  },
  "tasks": []
}

HDFS Connector:
Periodically polls data from given Kafka topic and writes them to HDFS.
Data from each Kafka topic is partitioned by the provided partitioner and divided into partition.
Each chunk of data is represented as an HDFS file with topic, kafka partition, start and end offsets of this data chunk in the filename. 
Default partition is the kafka partition
The connector automatically creates an external Hive partitioned table for each Kafka topic and updates the table according to the available data in HDFS.


 <parent>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-connect-storage-common-parent</artifactId>
        <version>5.0.0-SNAPSHOT</version>
  </parent>
  <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-storage-common</artifactId>
            <version>${confluent.version}</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-storage-core</artifactId>
            <version>${confluent.version}</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-storage-format</artifactId>
            <version>${confluent.version}</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-storage-partitioner</artifactId>
            <version>${confluent.version}</version>
        </dependency>
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-connect-storage-wal</artifactId>
            <version>${confluent.version}</version>
        </dependency>

commands:

install
```shell
sudo yum install kafka
```
the code is here 
```shell
ll /usr/hdp/3.0.0.0-698/kafka/bin/
total 132
-rwxr-xr-x. 1 root root 1816 Jan 10 15:04 connect-distributed.sh
-rwxr-xr-x. 1 root root 1813 Jan 10 15:04 connect-standalone.sh
```


1030  bin/kafka-topic.sh --create --zookeeper localhost:2181 --replication-factor 1 --partition 1 --topic test
 1031  bin/kafka-topic.sh --list --zookeeper localhost:2181
 1032  cat config/connect-file-source.properties
 1033  echo -e "foo\nbar" > test.txt
 1034  bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties
 1035  cat config/connect-file-source.properties
 1036  bin/kafka-topic.sh --list --zookeeper localhost:2181
 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-test --from-beginning
 1038  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic connect-test --from-beginning &
 1039  bin/connect-distributed.sh config/connect-standalone.properties config/connect-file-source.properties &


tmp_orders_topic

./bin/connect-distributed -daemon etc/kafka/connect-distributed.properties

./bin/connect-distributed etc/kafka/connect-distributed.properties

./bin/connect-distributed  config/connect-distributed.properties

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tmp_orders_topic --from-beginning

/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic sample

curl -X delete localhost:8083/connectors/hdfs-sink | jq


curl localhost:8083/connector-plugins | jq

curl localhost:8083/connectors/hdfs-sink/tasks | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100   323  100   323    0     0  35097      0 --:--:-- --:--:-- --:--:-- 40375
[
  {
    "id": {
      "connector": "hdfs-sink",
      "task": 0
    },
    "config": {
      "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
      "task.class": "io.confluent.connect.hdfs.HdfsSinkTask",
      "hadoop.conf.dir": "/usr/hdp/3.0.0.0-698/hadoop/conf",
      "flush.maxSize": "3",
      "tasks.max": "1",
      "topic": "test_hdfs",
      "hdfs.url": "hdfs://tmp/",
      "name": "hdfs-sink"
    }
  }
]
[sbouguerra@cn105-10 ~] curl -X Delete localhost:8083/connectors/hdfs-sink | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
  0     0    0     0    0     0      0      0 --:--:-- --:--:-- --:--:--     0
[sbouguerra@cn105-10 ~] curl localhost:8083/connectors


./bin/confluent load hdfs-sink-slim -d etc/kafka-connect-hdfs/quickstart-hdfs.properties
{
  "name": "hdfs-sink-2",
  "config": {
    "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
    "tasks.max": "3",
    "topic": "connect-test",
    "hdfs.url": "hdfs://cn105-10.l42scl.hortonworks.com:8020/tmp/slim2",
    "hadoop.conf.dir": "/usr/hdp/3.0.0.0-698/hadoop/conf",
    "flush.maxSize": "1",
    "hive.integration": "true",
    "schema.compatibility": "BACKWARD",
    "hive.metastore.uris": "thrift://localhost:9083"
  }
}


offset.flush.interval.ms
The frequency that the offsets are committed for source and sink connectors is controlled by the connector's offset.flush.interval.ms


/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic connect-configs && /usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic connect-offsets && 
/usr/hdp/current/kafka-broker/bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic connect-status


https://stackoverflow.com/questions/44871377/put-vs-flush-in-kafka-connector-sink-task

#automatic-retries
https://docs.confluent.io/current/connect/connect-elasticsearch/docs/elasticsearch_connector.html#automatic-retries

#Compactor and cleander code 
https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/txn/compactor/Cleaner.java
https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/txn/compactor/CompactorMR.java

# Storm Hive
https://github.com/apache/storm/tree/master/external/storm-hive

# Good implems or threads about how to do this 
https://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming/

https://github.com/jihoonson/druid/blob/b3c59eacf025c11bf214f48c2496150e65394ae9/extensions-core/kafka-indexing-service/src/main/java/io/druid/indexing/kafka/KafkaIndexTask.java

https://community.hortonworks.com/articles/49949/test-7.html

https://henning.kropponline.de/2015/01/24/hive-streaming-with-storm/


# Hive compactor
https://github.com/apache/hive/tree/master/ql/src/java/org/apache/hadoop/hive/ql/txn/compactor


