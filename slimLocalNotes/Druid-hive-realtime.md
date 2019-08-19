Need to build a mechanism to migrate older data from druid to llap.
	This can be as simple as adding a listner to the HS2 or what ever LLAP demoan manager where it can load the druid segment from HDFS and re-store it as an ORC historical data.
	This should be done via service discovery.

There is no update/delete on this table, since Druid doesn’t support it.
	Not sure what is the scop/use case of this. What does it mean to update a Kafka event? or Delete a Kafka Event. 
	Seems like most of the cases where ppls deal with a Stream they asume it is not a writable/editable?

If materialized views are used doing incremental updates for it may not be possible from Druid based tables because of absence of transaction ids etc
	What is the semantic of Transactions IDs? Druid uses Kafka Offsets to transactionaly commit segments. 

My questions:

How to manage the lifecyle of the ingestion task (Start/Configure/Stop/Status)?
	This need to have a carful design on how to delegate some part of Configuration and lifecyle management to Druid. 
	Ideal Hive should know about the existing Streams?
	Should hive define the Handoff strategy? 
	Shoudl hive manage the number of Druid Nodes.

Druid operations are quite opaque
	What indexing jobs are running?
	How to start / stop / delete jobs?
	Which jobs are having problems?

Those are my Questions and Cloudy thoughts about the document https://docs.google.com/document/d/18xEzblenPXBe-G1WA3mmohn18vEwz_5v51c1K6ZjQ-o/edit#

Question 1: Logical to physical tables planing/re-writing?
  First i think we need to agree that the user is interacting with one single logical table.
  Parts of the logical table is physically stored in Druid and other part is in Hive.
  Now the question is how to determine the physical partition that we need to hit when user query.
  Nishant proposed a watermark that is an absolute point in time implying that data older than watermark is in Hive 
  and vice versa. 
  My concern here is first, this doesn't take into account the late events that lands in Kafka then in Druid.
  Also this doesn't take into account the user inserted/updated data from the Hive side.
  
  One way to deal with this is the following:
    Rely on the metadata cache based on Druid Discovery APIs that has all the data served by Druid and Handed-off to Hive.
  
  Second dummy way is to assume that Druid always has some fraction of the data.
  This should be ok if we get the filter re-write to express the Druid query with accurate intervals or Selective filters. 
  Here am assuming will take advantage of Druid segment pruning and fast filtering for non existing dimensions (time). 
    
Question 2: is Historical in the Picture or not ?
 This question as very big implication, if we assume that Data has to be handed off to historical first then we can assume that 
 the data to be handed to Hive is already in HDFS or S3 and makes handoff more trivial.
 If not we need to have more checks and call backs to handle issues when flushing data from Realtime node
 to Deep Storage.
 
Question 3: We still need to agree if the user is allowed to insert/delete records from such table.
 
    
    
    
# HIVE only story
There is two aspect to the realtime streaming ingestion from streaming buffer eg Kafka to the Hive warehouse.
The first aspect is how to handle (start/pause/checkpoint/restart/stop/monitor/scale/errors_related_actions) for a long running indexing task.
The second aspect is to define the API and semantics of Realtime ingestion task managed by HIVE Acid streaming.

## Task Management
For Task managment aspect one we need to define a TaskManager (eg Overlord as per the Druid mythology) that will be in charge of the life cycle of tasks.
Not sure where is the best place for TasksManager to exists maybe the best place is HiveMetaStore. 
From HA perspective ideally TaskManager state can be lively replicated and periodically/atomically persisted to the Metastore and to other followers
From Scalability perspective we need to make sure that TaskManager can run as a singleton.

## Streaming Task
Looking at Kafka connect docs http://kafka.apache.org/documentation.html#connect seems like we can
leverage it and make Kafka manage the offsets and scaling up and down, while HIVE can take in charge
the committing of handoff via ACID. To answer to such questing need to read more about Hive Acid/Streaming/Hcat
and Dive into the KafkaConnect API and see what are the benefits of such model.

- Semantics of streaming/delivery
- Scalability
- Error handling
- Monitoring and status
- Lifecycle managements int/start/pause/checkpoint/resume/stop/clean     


# Testing 
```json
{"key":"value"}
{"key":"value2"}
{"key":"value3"}
{"key":"value1", "id":1}
{"key":"value2", "id":2}
{"key":"value3", "id":3}
```


```bash
mvn test -Dtest=TestMiniLlapLocalCliDriver  -Dtest.output.overwrite=true -Dqfile=kafkastorage_basic.q
mvn test -Dtest=TestMiniDruidKafkaCliDriver  -Dtest.output.overwrite=true -Dqfile=kafkastorage_basic.q
```
