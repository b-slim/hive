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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.JsonSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Generic Kafka Serde that allow user to delegate Serde to other class like Avro,
 * Json or any class that supports {@link BytesWritable}.
 */
public class GenericKafkaSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(GenericKafkaSerDe.class);
  // ORDER of fields and types matters here
  public static final ImmutableList<String>
      METADATA_COLUMN_NAMES =
      ImmutableList.of(KafkaStreamingUtils.PARTITION_COLUMN,
          KafkaStreamingUtils.OFFSET_COLUMN,
          KafkaStreamingUtils.TIMESTAMP_COLUMN);
  public static final ImmutableList<PrimitiveTypeInfo>
      METADATA_PRIMITIVE_TYPE_INFO =
      ImmutableList.of(TypeInfoFactory.intTypeInfo, TypeInfoFactory.longTypeInfo, TypeInfoFactory.longTypeInfo);

  private AbstractSerDe delegateSerDe;
  private ObjectInspector objectInspector;
  private final List<String> columnNames = Lists.newArrayList();
  StructObjectInspector delegateObjectInspector;

  //GenericDatumReader<GenericRecord> gdr;
  //DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
  private final UID uid = new UID();
  private Supplier<DatumReader<GenericRecord>> gdrSupplier;

  @Override public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
    final String className = tbl.getProperty(KafkaStreamingUtils.SERDE_CLASS_NAME, KafkaJsonSerDe.class.getName());
    delegateSerDe = createDelegate(className);
    delegateSerDe.initialize(conf, tbl);
    LOG.info("Using SerDe instance {}", delegateSerDe.getClass().getCanonicalName());
    if (!(delegateSerDe.getObjectInspector() instanceof StructObjectInspector)) {
      throw new SerDeException("Was expecting StructObject Inspector but have " + delegateSerDe.getObjectInspector()
          .getClass()
          .getName());
    }

    delegateObjectInspector = (StructObjectInspector) delegateSerDe.getObjectInspector();

    final List<ObjectInspector> inspectors;
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String
        columnNameDelimiter =
        tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ?
            tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER) :
            String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (!columnNameProperty.isEmpty()) {
      columnNames.addAll(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
    }

    columnNames.addAll(METADATA_COLUMN_NAMES);

    if (LOG.isDebugEnabled()) {
      LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
    }

    inspectors = new ArrayList<>(columnNames.size());
    inspectors.addAll(delegateObjectInspector.getAllStructFieldRefs()
        .stream()
        .map(structField -> structField.getFieldObjectInspector())
        .collect(Collectors.toList()));
    inspectors.addAll(METADATA_PRIMITIVE_TYPE_INFO.stream()
        .map(KafkaJsonSerDe.typeInfoToObjectInspector)
        .collect(Collectors.toList()));

    gdrSupplier = Suppliers.memoize(() -> {
      String schemaFromProperty = tbl.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), "");
      Preconditions.checkArgument(!schemaFromProperty.isEmpty(), "Avro Schema is empty Can not go further");
      Schema schema = AvroSerdeUtils.getSchemaFor(schemaFromProperty);
      return new SpecificDatumReader<>(schema);
    });

    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
  }

  private AbstractSerDe createDelegate(String className) {
    final Class<? extends AbstractSerDe> clazz;
    try {
      clazz = (Class<? extends AbstractSerDe>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      LOG.error("Failed a loading delegate SerDe {}", className);
      throw new RuntimeException(e);
    }
    // we are not setting conf thus null is okay
    return ReflectionUtil.newInstance(clazz, null);
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return delegateSerDe.getSerializedClass();
  }

  @Override public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    return delegateSerDe.serialize(obj, objInspector);
  }

  @Override public SerDeStats getSerDeStats() {
    return delegateSerDe.getSerDeStats();
  }

  @Override public Object deserialize(Writable blob) throws SerDeException {
    KafkaRecordWritable record = (KafkaRecordWritable) blob;
    // switch case the serde nature
    final Object row;
    if (delegateSerDe instanceof JsonSerDe) {
      // @TODO Text constructor copies the data, this op is not needed
      row = delegateSerDe.deserialize(new Text(record.getValue()));
    } else if (delegateSerDe instanceof AvroSerDe) {
      AvroGenericRecordWritable avroGenericRecordWritable = new AvroGenericRecordWritable();
      try {
        GenericRecord
            avroRecord =
            gdrSupplier.get().read(null, DecoderFactory.get().binaryDecoder(record.getValue(), null));
        avroGenericRecordWritable.setRecord(avroRecord);
        avroGenericRecordWritable.setRecordReaderID(uid);
        avroGenericRecordWritable.setFileSchema(avroRecord.getSchema());
      } catch (IOException e) {
        throw new SerDeException(e);
      }
      row = delegateSerDe.deserialize(avroGenericRecordWritable);
    } else {
      // default assuming delegate Serde know how to deal with
      row = delegateSerDe.deserialize(new BytesWritable(record.getValue()));
    }

    return columnNames.stream().map(name -> {
      switch (name) {
      case KafkaStreamingUtils.PARTITION_COLUMN:
        return new IntWritable(record.getPartition());
      case KafkaStreamingUtils.OFFSET_COLUMN:
        return new LongWritable(record.getOffset());
      case KafkaStreamingUtils.TIMESTAMP_COLUMN:
        return new LongWritable(record.getTimestamp());
      default:
        return delegateObjectInspector.getStructFieldData(row, delegateObjectInspector.getStructFieldRef(name));
      }
    }).collect(Collectors.toList());
  }

  @Override public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
