/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.kafka;

import org.apache.hadoop.io.Writable;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Writable implementation of Kafka ConsumerRecord.
 * Serialized in the form
 * kafkaRecordTimestamp(long) | kafkaPartition (int) | recordOffset (long) | value.size (int) | value (byte [])
 */
public class KafkaRecordWritable implements Writable {

  private int partition;
  private long offset;
  private long timestamp;
  private byte[] value;

  static KafkaRecordWritable fromKafkaRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    return new KafkaRecordWritable(consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        consumerRecord.value());
  }

  void set(ConsumerRecord<byte[], byte[]> consumerRecord) {
    this.partition = consumerRecord.partition();
    this.timestamp = consumerRecord.timestamp();
    this.offset = consumerRecord.offset();
    this.value = consumerRecord.value();
  }

  private KafkaRecordWritable(int partition, long offset, long timestamp, byte[] value) {
    this.partition = partition;
    this.offset = offset;
    this.timestamp = timestamp;
    this.value = value;
  }

  @SuppressWarnings("WeakerAccess") public KafkaRecordWritable() {
  }

  @Override public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(timestamp);
    dataOutput.writeInt(partition);
    dataOutput.writeLong(offset);
    dataOutput.writeInt(value.length);
    dataOutput.write(value);
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    timestamp = dataInput.readLong();
    partition = dataInput.readInt();
    offset = dataInput.readLong();
    int size = dataInput.readInt();
    if (size > 0) {
      value = new byte[size];
      dataInput.readFully(value);
    } else {
      value = new byte[0];
    }
  }

  int getPartition() {
    return partition;
  }

  long getOffset() {
    return offset;
  }

  long getTimestamp() {
    return timestamp;
  }

  byte[] getValue() {
    return value;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaRecordWritable)) {
      return false;
    }
    KafkaRecordWritable that = (KafkaRecordWritable) o;
    return getPartition() == that.getPartition()
        && getOffset() == that.getOffset()
        && getTimestamp() == that.getTimestamp()
        && Arrays.equals(getValue(), that.getValue());
  }

  @Override public int hashCode() {

    int result = Objects.hash(getPartition(), getOffset(), getTimestamp());
    result = 31 * result + Arrays.hashCode(getValue());
    return result;
  }

}
