/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;


/**
 * Kafka puller input format is in charge of reading a exact set of records from a Kafka Queue
 * The input split will contain the set of topic partition and start/end offsets
 * Records will be returned as bytes
 */
public class KafkaPullerInputFormat extends InputFormat<NullWritable, KafkaRecordWritable>
    implements org.apache.hadoop.mapred.InputFormat<NullWritable, KafkaRecordWritable>
{

  public static final String HIVE_KAFKA_TOPIC = "kafka.topic";
  public static final String CONSUMER_CONFIGURATION_PREFIX = "kafka.consumer";
  public static final String HIVE_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String GENERATION_TIMEOUT_MS = "hive.kafka.split.generation.timeout.ms";

  private static final Logger log = LoggerFactory.getLogger(KafkaPullerInputFormat.class);


  @Override
  public InputSplit[] getSplits(
      JobConf jobConf, int i
  ) throws IOException
  {
    List<KafkaPullerInputSplit> inputSplits = null;
    try {
      inputSplits = computeSplits(jobConf);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
    InputSplit[] inputSplitsArray = new InputSplit[inputSplits.size()];
    return inputSplits.toArray(inputSplitsArray);
  }

  /**
   * Build a full scan using Kafka list partition then beginning/end offsets
   * This function might block duo to calls like
   * org.apache.kafka.clients.consumer.KafkaConsumer#beginningOffsets(java.util.Collection)
   *
   * @param topic      kafka topic
   * @param consumer   initialized kafka consumer
   * @param tablePaths hive table path
   *
   * @return full scan input split collection based on Kafka metadata APIs
   */
  private static List<KafkaPullerInputSplit> buildFullScanFromKafka(
      String topic,
      KafkaConsumer consumer,
      Path[] tablePaths
  )
  {
    final Map<TopicPartition, Long> starOffsetsMap;
    final Map<TopicPartition, Long> endOffsetsMap;

    final List<TopicPartition> topicPartitions;
    topicPartitions = fetchTopicPartitions(topic, consumer);
    starOffsetsMap = consumer.beginningOffsets(topicPartitions);
    endOffsetsMap = consumer.endOffsets(topicPartitions);

    if (log.isDebugEnabled()) {
      log.info(
          "Found the following partitions [{}]",
          topicPartitions.stream().map(topicPartition -> topicPartition.toString())
                         .collect(Collectors.joining(","))
      );
      starOffsetsMap.forEach((tp, start) -> log.info("TPartition [{}],Start offsets [{}]", tp, start));
      endOffsetsMap.forEach((tp, end) -> log.info("TPartition [{}],End offsets [{}]", tp, end));
    }
    return topicPartitions.stream().map(
        topicPartition -> new KafkaPullerInputSplit(
            topicPartition.topic(),
            topicPartition.partition(),
            starOffsetsMap.get(topicPartition),
            endOffsetsMap.get(topicPartition),
            tablePaths[0]
        )).collect(Collectors.toList());
  }

  private List<KafkaPullerInputSplit> computeSplits(Configuration configuration)
      throws IOException, InterruptedException
  {
    // this will be used to harness some KAFKA blocking calls
    final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();
    try (KafkaConsumer consumer = new KafkaConsumer(KafkaStreamingUtils.consumerProperties(configuration))) {
      final String topic = configuration.get(HIVE_KAFKA_TOPIC);
      final long timeoutMs = configuration.getLong(GENERATION_TIMEOUT_MS, 5000);
      // hive depends on FileSplits
      JobConf jobConf = new JobConf(configuration);
      Path[] tablePaths = org.apache.hadoop.mapred.FileInputFormat.getInputPaths(jobConf);

      Future<List<KafkaPullerInputSplit>> futureFullHouse = EXECUTOR.submit(() -> buildFullScanFromKafka(
          topic,
          consumer,
          tablePaths
      ));
      List<KafkaPullerInputSplit> fullHouse;
      try {
        fullHouse = futureFullHouse.get(timeoutMs, TimeUnit.MILLISECONDS);
      }
      catch (TimeoutException | ExecutionException e) {
        futureFullHouse.cancel(true);
        log.error("can not generate full scan split", e);
        // at this point we can not go further fail split generation
        throw new IOException(e);
      }


      final ImmutableMap.Builder<TopicPartition, KafkaPullerInputSplit> builder = new ImmutableMap.Builder();
      fullHouse.stream().forEach(input -> builder.put(new TopicPartition(
          input.getTopic(),
          input.getPartition()
      ), input));

      final KafkaScanTrimmer kafkaScanTrimmer = new KafkaScanTrimmer(builder.build(), consumer);
      final String filterExprSerialized = configuration.get(TableScanDesc.FILTER_EXPR_CONF_STR);

      if (filterExprSerialized != null && !filterExprSerialized.isEmpty()) {
        ExprNodeGenericFuncDesc filterExpr = SerializationUtilities.deserializeExpression(filterExprSerialized);
        log.info("Kafka trimmer working on Filter tree {}", filterExpr.getExprString());
        Callable<List<KafkaPullerInputSplit>> trimmerWorker = () -> kafkaScanTrimmer.computeOptimizedScan(filterExpr)
                                                                                    .entrySet()
                                                                                    .stream()
                                                                                    .map(entry -> entry.getValue())
                                                                                    .collect(Collectors.toList());

        Future<List<KafkaPullerInputSplit>> futureTinyHouse = EXECUTOR.submit(trimmerWorker);
        try {
          return futureTinyHouse.get(timeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
          futureTinyHouse.cancel(true);
          log.error("Had issue with trimmer will return full scan ", e);
          return fullHouse;
        }
      }
      //Case null: it can be filter evaluated to false or no filter at all thus return full scan
      return fullHouse;
    }
    finally {
      EXECUTOR.shutdown();
    }
  }


  private static List<TopicPartition> fetchTopicPartitions(String topic, KafkaConsumer consumer)
  {
    // this will block till REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms"
    // then throws org.apache.kafka.common.errors.TimeoutException if can not fetch metadata
    // @TODO add retry logic maybe
    List<PartitionInfo> partitions = consumer.partitionsFor(topic);
    return partitions.stream().map(p -> new TopicPartition(topic, p.partition()))
                     .collect(Collectors.toList());
  }

  @Override
  public RecordReader<NullWritable, KafkaRecordWritable> getRecordReader(
      InputSplit inputSplit,
      JobConf jobConf, Reporter reporter
  ) throws IOException
  {
    return new KafkaPullerRecordReader((KafkaPullerInputSplit) inputSplit, jobConf);
  }

  @Override
  public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(
      JobContext jobContext
  ) throws IOException, InterruptedException
  {
    return computeSplits(jobContext.getConfiguration()).stream()
                                                       .map(kafkaPullerInputSplit -> (org.apache.hadoop.mapreduce.InputSplit) kafkaPullerInputSplit)
                                                       .collect(Collectors.toList());
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordReader<NullWritable, KafkaRecordWritable> createRecordReader(
      org.apache.hadoop.mapreduce.InputSplit inputSplit, TaskAttemptContext taskAttemptContext
  ) throws IOException, InterruptedException
  {
    return new KafkaPullerRecordReader();
  }
}
